import itertools
import logging
from typing import *

from pyspark.sql import DataFrame

from .aggregation_node import AggregationNode
from .gma_exceptions import LogicSyntaxError
from .logic_node import LogicNode
from .mutable_dataframe import MutableDataFrame
from .node import Node, NodeSet

logger = logging.getLogger(__name__)


# only used for troubleshooting optimizer
DEBUG_OPTMIZER = False


class Logic:

    def __init__(self, logic_definition, input_data: DataFrame, context: Dict=None, final_level: str='',
                 strict_syntax=True, bypass_optimizer=False, checkpoint_interval=0):
        self._input_data = input_data
        self.context = context or {}
        self._df = MutableDataFrame(self._input_data, checkpoint_interval, self.context)
        self.logic_tree = self.load_config(logic_definition, strict_syntax)
        self.final_level = final_level
        self.bypass_optimizer = bypass_optimizer

    def load_config(self, logic_definition, strict_syntax):
        logic_tree, errors = loads(logic_definition, self._df)
        if errors:
            for error in errors:
                logger.warn(error)
            if strict_syntax:
                raise LogicSyntaxError('There is a logic syntax error', errors)
        return logic_tree

    def flattened(self) -> List[Node]:
        """ Flattens tree of logic nodes into a list """
        logger.info('Flattening tree')

        def flatten(node):
            return list(itertools.chain(*[flatten(child_tree) for child_tree in node._children])) + [node]

        flattened_nodes = list(itertools.chain(*[flatten(node) for node in self.logic_tree]))
        logger.info('{0} nodes found in logic'.format(len(flattened_nodes)))

        logger.debug('flattened complete')
        return flattened_nodes

    def optimized(self) -> List[Node]:
        """ Puts nodes and dependencies in a graph, then sorts by level and aggregation bool to create a
            sequence that minimizes level changes"""

        logger.info('Running dag optimization')

        nodes = self.flattened()

        if self.bypass_optimizer:
            logger.info('Bypassing optimizer')
            return nodes

        # Make a graph list and initialize degrees
        dag = get_dag(nodes)
        dag = update_degrees(dag)

        if DEBUG_OPTMIZER:
            logger.debug('\nNodes before sorting\n{}'.format('\n'.join([x._to_string() for x in nodes])))
    
        # Get the nodes sorted by degree (number of dependencies)
        sorted_dag = sorted(dag, key=node_sort_function)

        # Initialize the empty final dag
        final_dag = []

        # Iterate over the sorted dag
        while sorted_dag:
            # Take the first node from the sorted dag
            current_node = sorted_dag.pop(0)

            # Add it to the final dag
            # if current_node['degree'] > 0:
                #raise ValueError('Problem resolving dependencies for {0}'.format(current_node['name']))
            final_dag.append(current_node)

            # Update the degrees of the sorted dag
            sorted_dag = update_degrees(sorted_dag, final_dag)

            # Re-sort the dag based on the level of the current node
            sorted_dag = sorted(sorted_dag, key=lambda x: node_sort_function(x, current_node['level'], current_node['aggregate']))

        # Get the full nodes from the sorted names
        sorted_node_names = [x['name'] for x in final_dag]
        nodes_map = {x.name: x for x in nodes}
        nodes = [nodes_map[x] for x in sorted_node_names]

        if DEBUG_OPTMIZER:
            logger.debug('\nNodes after optimizer\n{}'.format('\n'.join([x._to_string() for x in nodes])))

        logger.debug('optimization complete')
        return nodes

    def deduped(self) -> List[Node]:
        """ Iterates over nodes, collapsing matching function/params into a single node with alias """
        logger.debug('deduped function starting')

        optimized_nodes = self.optimized()

        logger.debug('deduplicating nodes')
        dedupe_map = {}
        dedupe_list = []
        for node in optimized_nodes:
            if node.is_aggregation:
                # aggregations don't get deduped
                dedupe_list.append(node)
            elif node.description in dedupe_map:
                # if the node, which is not an aggregation, has already been
                # 'captured' then it can be deduped
                # so add it as an alias to the previously captured node
                dedupe_map[node.description].add_alias(node)
            else:
                # haven't seen this node before
                dedupe_map[node.description] = node
                dedupe_list.append(node)

        logger.info('{0} nodes after dedupe'.format(len(dedupe_list)))

        if DEBUG_OPTMIZER:
            logger.debug('\nNodes after dedupe\n{}'.format('\n'.join([x._to_string() for x in dedupe_list])))

        logger.debug('deduped function complete')
        return dedupe_list

    def batched(self):
        """ Combines the nodes into sets with the same level and aggregation bool """
        logger.debug('batch function starting')
        nodes = self.deduped()

        node_sets = []
        level = None
        aggregate_bool = None

        logger.debug('batching nodes')
        for node in nodes:
            if node.level != level or node.is_aggregation != aggregate_bool:
                level = node.level
                aggregate_bool = node.is_aggregation
                node_sets.append(NodeSet())

            node_sets[-1].add_node(node)

        if DEBUG_OPTMIZER:
            debug_output = ['\nBatched Nodes','-'*40]
            for index, node_set in enumerate(node_sets):
                debug_output.append('Node {} of {}'.format(index, len(node_sets)))
                for sub_index, node in enumerate(node_set.subnodes):
                    debug_output.append('SubNode {} of {}\n{}'.format(sub_index, len(node_set.subnodes), node._to_string('  ')))
            logger.debug('\n'.join(debug_output))

        logger.info('{0} batches created'.format(len(node_sets)))
        logger.debug('batch function complete')
        return node_sets

    def apply(self) -> DataFrame:
        logger.debug('apply function starting')

        # Iterate through node sets
        node_sets = self.batched()
        node_sets_cnt = len(node_sets)
        for index, node_set in enumerate(node_sets, 1):
            logger.debug('logic.apply - processing node {} of {}'.format(index, node_sets_cnt))
            logger.info('Applying node set at level {0}'.format(node_set.level))
            self._df.apply_logic(node_set)

        # Return df to top level
        self._df.adjust_level(self.final_level)

        logger.debug('apply function complete')
        return self._df._df


def loads(data: Union[List[Dict], Dict], input_data: MutableDataFrame=None) -> Tuple[List[LogicNode], List[str]]:
    defined_names = []

    def build_node(node_def: Dict, default_name: str=None, parent: LogicNode=None):

        # Get name and check for uniqueness among nodes
        name = node_def.get('name', default_name)
        if name in defined_names:
            raise LogicSyntaxError('Duplicate names', ['Duplicate name fields are not allowed. Name collision on: {}'.format(name)])
        defined_names.append(name)

        # If there's an aggregation, create aggregation node and add the logic node as a child
        if 'agg_function' in node_def:
            node = AggregationNode(default_name, input_data, parent, **node_def)
            node_def['name'] = node.pre_agg_name if node._has_explicit_name else None
            if 'concept' not in node_def:
                raise LogicSyntaxError('Node error', ["Error processing node {0} - can't define aggregate function on node without explicit struct concept".format(node.name or default_name)])
            subnode = LogicNode(node.pre_agg_name, input_data, parent, **node_def)

            # Iterate over sublogic nodes and add as children of node
            sublogic = node_def.get('sublogic')
            if sublogic:
                for index, subnode_def in enumerate(sublogic):
                    # the name field is implicit.  add a properly defined name to the sublogic
                    default_name = '{}_sub_{}'.format(node.name, index)
                    subnode.add_node(build_node(subnode_def, default_name=default_name, parent=subnode))

            node.add_node(subnode)
        else:
            # Otherwise, just create the logic node
            node = LogicNode(default_name, input_data, parent, **node_def)

            # Iterate over sublogic nodes and add as children of node
            sublogic = node_def.get('sublogic')
            if sublogic:
                for index, subnode_def in enumerate(sublogic):
                    # the name field is implicit.  add a properly defined name to the sublogic
                    default_name = '{}_sub_{}'.format(node.name, index)
                    node.add_node(build_node(subnode_def, default_name=default_name, parent=node))

        return node

    if isinstance(data, dict):
        # convert the single dict to a list
        data = [data]

    node_errors = []
    nodes = []
    for node_definition in data:
        if 'name' not in node_definition:
            node_errors.append('Root nodes must have an explicitly defined name field.  def: {}'.format(node_definition))
            raise ValueError('Root nodes must have an explicitly defined name field')
        try:
            nodes.append(build_node(node_definition))
        except LogicSyntaxError as e:
            node_errors = [*node_errors, *e.errors]

    def validate_node(node, node_errors):
        try:
            node.func.validate_params(node.func_params)
        except LogicSyntaxError as e:
            node_errors = [*node_errors, *e.errors]
        for child_node in node._children:
            validate_node(child_node, node_errors)

    for node in nodes:
        validate_node(node, node_errors)

    return nodes, node_errors


def get_dag(nodes: List[Node]) -> List[dict]:
    node_names = [x.name for x in nodes]

    def make_node(x):
        return {'name': x.name,
                'level': x.level,
                'dependencies': [x for x in list(set([str(y) for y in x.dependencies])) if x in node_names],
                'aggregate': x.is_aggregation}

    return [make_node(x) for x in nodes]


def update_degrees(nodes: List[dict], done: List[dict]=None) -> List[dict]:
    """ Takes a list of node dictionaries and returns a new nodes list with
    degree attribute updated to reflect nodes already accounted for """

    done_names = [x['name'] for x in done or []]

    def new_node(x):
        return {'name': x['name'],
                'level': x['level'],
                'dependencies': x['dependencies'],
                'degree': len([x for x in x['dependencies'] if x not in done_names]),
                'aggregate': x['aggregate']}

    vals = [new_node(x) for x in nodes]
    return vals


def node_sort_function(node, level=None, aggregate=None):
    """ Sort the nodes based on their degree, whether their level matches
    the current level, and whether they're an aggregation """
    if level:
        # Prefer nodes at the same level
        level_diff = node['level'] != level
        # If the level is the same, prefer nodes with same agg status, otherwise prefer non-agg nodes
        agg_pref = node['aggregate'] if level_diff else node['aggregate'] != aggregate
        return node['degree'], level_diff, agg_pref
    else:
        return node['degree'], int(node['aggregate'])