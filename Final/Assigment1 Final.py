# Databricks notebook source
# William Tandio
# Assignment 1

# Creating DBMS Folder
dbutils.fs.mkdirs("FileStore/tables/assignment1")
dbutils.fs.cp("FileStore/tables/geoPoints0.csv","FileStore/tables/assignment1")
dbutils.fs.cp("FileStore/tables/geoPoints1.csv","FileStore/tables/assignment1")

# COMMAND ----------

# Trying with Brute Force
sc = spark.sparkContext

data = sc.textFile("FileStore/tables/assignment1")

# extract the point, x and y values
data = data.map(lambda line: line.split(","))
data = data.map(lambda x: (x[0], float(x[1]), float(x[2])))

# euclidean distance formula
def calc_distance(pt1, pt2):
    return ((pt1[1]-pt2[1])**2+(pt1[2]-pt2[2])**2)**0.5

# find close pairs
def close_pairs(pointa, pointb, threshold):
    close_pairs = []
    for i in pointb:
        distance = calc_distance(pointa, i)
        if distance <= threshold: 
            close_pairs.append((pointa[0], i[0]))
    return close_pairs

# threshold value can be adjusted
threshold = 0.75

# get all close pairs
#get all possible combination
cartesian = data.cartesian(data)
#removing the duplicates
result = cartesian.filter(lambda x: x[0][0] < x[1][0])
#applying the function into the rdd
#print(result.collect())
finalresult = result.flatMap(lambda x: close_pairs(x[0], [x[1]], threshold))


# convert the close pairs to a list
close_pairs_list = finalresult.collect()
print('Brute Force with Cartesian')
print(len(close_pairs_list), close_pairs_list)

#######################Brute Force with Loop############################
sc = spark.sparkContext

data = sc.textFile("FileStore/tables/assignment1")

# extract the point, x and y values
data = data.map(lambda line: line.split(","))
data = data.map(lambda x: (x[0], float(x[1]), float(x[2])))
points_list = data.collect()

def euclidean_distance(pt1, pt2):
    return ((pt1[1]-pt2[1])**2+(pt1[2]-pt2[2])**2)**0.5

def calculate_distances(points_list, threshold):
    distances = []
    for i in range(len(points_list)):
        for j in range(i+1, len(points_list)): 
            distance = euclidean_distance(points_list[i], points_list[j])
            if distance < threshold:
                distances.append((points_list[i][0], points_list[j][0]))
    return distances

threshold = 0.75
result = calculate_distances(points_list, threshold)
print('Brute Force with For Loop')
print(len(result),result)

# COMMAND ----------

# Forming Grid Method
sc = spark.sparkContext

data = sc.textFile("FileStore/tables/assignment1")

# extract the point, x and y values
data = data.map(lambda line: line.split(","))
data = data.map(lambda x: (x[0], float(x[1]), float(x[2])))

# threshold distance
threshold_distance = 0.75

# grid cell size
cell_size = 0.75


########################### FORMING GRID ##############################
# Assign each point to a grid cell
def grid_cell(point):
    x, y = point[1], point[2]
    i = int(x / cell_size)
    j = int(y / cell_size)
    return (i, j), (point[0], x, y)


#Neighbor Function, grabbing all cells in the direction of "down right", including its cell
def neighbor(cell):
    neighbors = [cell]
    i, j = cell
    #Creating neighbor list
    neighbors.append((i-1,j-1))
    neighbors.append((i,j-1))
    neighbors.append((i+1,j-1))
    neighbors.append((i+1,j))
    return neighbors
    
#Create RDD with cell as keys and point information as values, group it so each key have points in 1 cell
#the mapValues is only to show the value for print
gridcell = data.map(grid_cell)
#print(gridcell.collect())

#The format is (All of the neighbors list),((Home Node),(Point1,X,Y))
neighborcell = gridcell.map(lambda x: (neighbor(x[0]),x))
#print(neighborcell.collect())

#Push the neighbor looping the neighbors list and pair it with the cell, where each cell also consist of its home node
flatneighbor = neighborcell.flatMap(lambda x: ((i,x[1]) for i in x[0]))
#print(flatneighbor.collect())

# Groupbykey
groupedcell = flatneighbor.groupByKey().mapValues(list)



#################### CALCULATION PART #######################

#Distance Formula
def euclidean_distance(cell1, cell2):
    pt1 = cell1[1]
    pt2 = cell2[1]
    print(pt1,pt2)
    dist = ((pt1[1]-pt2[1])**2+(pt1[2]-pt2[2])**2)**0.5
    return dist

#Cells in this function is the whole information of ((Current Node being calculated),[List of all cell that are being pushed into this node with their original home node])
#Example "Cells": ((4, 0), [((4, 0), ('Pt00', 3.49838, 0.662006)), ((3, 1), ('Pt19', 2.33941, 0.867107))])
def calc_distances(cells):
    current_node = cells[0]
    points_list = cells[1]
    distances = []
    for i in range(len(points_list)):
        for j in range(i+1, len(points_list)): 
            home_node1 = points_list[i][0]
            home_node2 = points_list[j][0]
            #Calculate only if neither of the node if from home node.
            #Avoid duplicates that calculate points that are not from this node
            if home_node1 == current_node or home_node2 == current_node:
                distance = euclidean_distance(points_list[i], points_list[j])
                if distance < threshold_distance:
                    distances.append((points_list[i][1][0], points_list[j][1][0]))
                    print('append!')          
    return distances


#result = groupedcell.map(lambda x:(calc_distances(x[1])))
result = groupedcell.flatMap(lambda x:(calc_distances(x)))
print(result.count(),result.collect())
