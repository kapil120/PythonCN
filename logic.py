# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 23:04:01 2020

@author: Kapil
"""
'''
import random
def start_game():
    mat=[]
    for i in range(4):
        mat.append([0]*4)
    return mat

def add_new_2(mat):
    
    r=random.randint(0,3)  #r=row
    c=random.randint(0,3)  #c=column
    
    while(mat[r][c]!=0):
        r=random.randint(0,3)  #r=row
        c=random.randint(0,3)  #c=column
    
    mat[r][c]=2

def compress(mat):
    changed=False
    new_mat=[]
    for i in range(4):
        new_mat.append([0]*4)
    
    for i in range(4):
        pos=0
        for j in range(4):
            if mat[i][j]!=0:
                new_mat[i][pos]=mat[i][j]
                if  j!=pos:
                    changed=True
                pos+=1
    return new_mat,changed

def merge(mat):
    
    changed=False
    
    for i in range(4):
        for j in range(3):
            if mat[i][j]==mat[i][j+1] and mat[i][j]!=0:
                mat[i][j]=mat[i][j]*2
                mat[i][j+1]=0
                changed=True
    return mat,changed

def reverse(mat):
    
    new_mat=[]
    
    for i in range(4):
        new_mat.append([])
        for j in range(4):
            new_mat[i].append(mat[4-j-1])
            
    return new_mat


def transpose(mat):
    
    new_mat=[]
    
    for i in range(4):
        new_mat.append([])
        for j in range(4):
            new_mat[i].append(mat[j][i])
            
    return new_mat  

def move_up(grid):
    transposed_grid=transpose(grid)
    new_grid,changed1=compress(transposed_grid)
    new_grid,changed2=merge(new_grid)
    changed=changed1 or changed2
    new_grid=compress(new_grid)
    final_grid=transpose(new_grid)
    return final_grid,changed


def move_down(grid):
    transposed_grid=transpose(grid)
    reversed_grid=reverse(transposed_grid)
    new_grid,changed1=compress(reversed_grid)
    new_grid,changed2=merge(new_grid)
    changed=changed1 or changed2
    new_grid=compress(new_grid)
    final_reversed_grid=reverse(new_grid)
    final_grid=transpose(final_reversed_grid)
    return final_grid,changed

def move_right(grid):
    
    reversed_grid=reverse(grid)
    new_grid,changed1=compress(reversed_grid)
    new_grid,changed2=merge(new_grid)
    changed=changed1 or changed2
    new_grid=compress(new_grid)
    final_grid=reverse(new_grid)
    
    return final_grid,changed
    

def move_left(grid):
    
    new_grid,changed1=compress(grid)
    new_grid,changed2=merge(new_grid)
    changed=changed1 or changed2
    new_grid,temp=compress(new_grid)
    return new_grid,changed
   
def get_current_state(mat):
    
    #Anywhere 2048 Is Present
    for i in range(4):
        for j in range(4):
            if mat[i][j]==2048:
                return "WON"
   
    #Anywhere 0 Is Present
    for i in range(4):
        for j in range(4):
            if mat[i][j]==0:
                return "GAME NOT OVER"
    
    #For Every Row and Column Except Last Row And Last Column   
    for i in range(3):
        for j in range(3):
            if (mat[i][j]==mat[i+1][j] or mat[i][j]==mat[i][j+1]):
                return "GAME NOT OVER"
    
    #For last Row    
    for j in range(3):
        if mat[3][j]==mat[3][j+1]:
            return "GAME NOT OVER"
    
    #For Last Column
    for i in range(3):
        if mat[i][3]==mat[i+1][3]:
            return "GAME NOT OVER"
    
    return "LOST"

mat=start_game()
print(mat)
add_new_2(mat)
add_new_2(mat)
mat=move_up(mat)
print(mat)
        '''
# CS1010FC --- Programming Methodology
#
# Mission N Solutions
#
# Note that written answers are commented out to allow us to run your
# code easily while grading your problem set.
import random
import constants as c

#######
#Task 1a#
#######

# [Marking Scheme]
# Points to note:
# Matrix elements must be equal but not identical
# 1 mark for creating the correct matrix


def new_game(n):
    matrix = []

    for i in range(n):
        matrix.append([0] * n)
    return matrix

###########
# Task 1b #
###########

# [Marking Scheme]
# Points to note:
# Must ensure that it is created on a zero entry
# 1 mark for creating the correct loop


def add_two(mat):
    a = random.randint(0, len(mat)-1)
    b = random.randint(0, len(mat)-1)
    while(mat[a][b] != 0):
        a = random.randint(0, len(mat)-1)
        b = random.randint(0, len(mat)-1)
    mat[a][b] = 2
    return mat

###########
# Task 1c #
###########

# [Marking Scheme]
# Points to note:
# Matrix elements must be equal but not identical
# 0 marks for completely wrong solutions
# 1 mark for getting only one condition correct
# 2 marks for getting two of the three conditions
# 3 marks for correct checking


def game_state(mat):
    for i in range(len(mat)):
        for j in range(len(mat[0])):
            if mat[i][j] == 2048:
                return 'win'
    for i in range(len(mat)-1):
        # intentionally reduced to check the row on the right and below
        # more elegant to use exceptions but most likely this will be their solution
        for j in range(len(mat[0])-1):
            if mat[i][j] == mat[i+1][j] or mat[i][j+1] == mat[i][j]:
                return 'not over'
    for i in range(len(mat)):  # check for any zero entries
        for j in range(len(mat[0])):
            if mat[i][j] == 0:
                return 'not over'
    for k in range(len(mat)-1):  # to check the left/right entries on the last row
        if mat[len(mat)-1][k] == mat[len(mat)-1][k+1]:
            return 'not over'
    for j in range(len(mat)-1):  # check up/down entries on last column
        if mat[j][len(mat)-1] == mat[j+1][len(mat)-1]:
            return 'not over'
    return 'lose'

###########
# Task 2a #
###########

# [Marking Scheme]
# Points to note:
# 0 marks for completely incorrect solutions
# 1 mark for solutions that show general understanding
# 2 marks for correct solutions that work for all sizes of matrices


def reverse(mat):
    new = []
    for i in range(len(mat)):
        new.append([])
        for j in range(len(mat[0])):
            new[i].append(mat[i][len(mat[0])-j-1])
    return new

###########
# Task 2b #
###########

# [Marking Scheme]
# Points to note:
# 0 marks for completely incorrect solutions
# 1 mark for solutions that show general understanding
# 2 marks for correct solutions that work for all sizes of matrices


def transpose(mat):
    new = []
    for i in range(len(mat[0])):
        new.append([])
        for j in range(len(mat)):
            new[i].append(mat[j][i])
    return new

##########
# Task 3 #
##########

# [Marking Scheme]
# Points to note:
# The way to do movement is compress -> merge -> compress again
# Basically if they can solve one side, and use transpose and reverse correctly they should
# be able to solve the entire thing just by flipping the matrix around
# No idea how to grade this one at the moment. I have it pegged to 8 (which gives you like,
# 2 per up/down/left/right?) But if you get one correct likely to get all correct so...
# Check the down one. Reverse/transpose if ordered wrongly will give you wrong result.


def cover_up(mat):
    new = []
    for j in range(c.GRID_LEN):
        partial_new = []
        for i in range(c.GRID_LEN):
            partial_new.append(0)
        new.append(partial_new)
    done = False
    for i in range(c.GRID_LEN):
        count = 0
        for j in range(c.GRID_LEN):
            if mat[i][j] != 0:
                new[i][count] = mat[i][j]
                if j != count:
                    done = True
                count += 1
    return (new, done)


def merge(mat):
    done = False
    for i in range(c.GRID_LEN):
        for j in range(c.GRID_LEN-1):
            if mat[i][j] == mat[i][j+1] and mat[i][j] != 0:
                mat[i][j] *= 2
                mat[i][j+1] = 0
                done = True
    return (mat, done)


def up(game):
    print("up")
    # return matrix after shifting up
    game = transpose(game)
    game, done = cover_up(game)
    temp = merge(game)
    game = temp[0]
    done = done or temp[1]
    game = cover_up(game)[0]
    game = transpose(game)
    return (game, done)


def down(game):
    print("down")
    game = reverse(transpose(game))
    game, done = cover_up(game)
    temp = merge(game)
    game = temp[0]
    done = done or temp[1]
    game = cover_up(game)[0]
    game = transpose(reverse(game))
    return (game, done)


def left(game):
    print("left")
    # return matrix after shifting left
    game, done = cover_up(game)
    temp = merge(game)
    game = temp[0]
    done = done or temp[1]
    game = cover_up(game)[0]
    return (game, done)


def right(game):
    print("right")
    # return matrix after shifting right
    game = reverse(game)
    game, done = cover_up(game)
    temp = merge(game)
    game = temp[0]
    done = done or temp[1]
    game = cover_up(game)[0]
    game = reverse(game)
    return (game, done)