#!/usr/bin/python3
import math

# cost_index : Btree index cost estimation
# source code : src/backend/optimizer/path/costsize.c
# version : REL_13_4


##########################################################################
################################   动态参数  ##############################
##########################################################################


## 索引的选择性 pg_stats表
btreeSelectivity=1  

## 索引相关性。主键扫描默认是1。索引的顺序与主表数据排列顺序的关联性，用来描述通过索引扫描数据时，回表的时候的顺序读的概率
indexCorrelation=1  

## 条件列的selectivity * correlation，pg_stats的common frequence 或者 histogram计算出来
matched_selectivity=0.0002 * 0.9061121   

baserel_tuples=2541254    # 主表的tuples数量
index_tuples=2541254      # 索引的tuples数量
index_pages=6970          # 索引的pages数量
baserel_pages=13737       # 主表的pages数量
index_tree_height = 2     # 索引btree的高度


# effective cache size
effective_cache_size=16*1024*1024*1024


##########################################################################
###########################   configurations  ############################
##########################################################################

## configurations in postgresql.conf
cpu_index_tuple_cost = 0.005
qual_op_cost = 0 # 0.0025
cpu_per_tuple = 0.0125  # 0.01
random_page_cost = 4    # default
seq_page_cost = 1.0     # default
cpu_operator_cost = 0.0025




##########################################################################
#######################   copy from postgreSQL  ##########################
##########################################################################


class GenericCosts:
    indexStartupCost=0.0
    indexTotalCost=0.0
    indexSelectivity=0.0
    indexCorrelation=0.0
    numIndexPages=0.0
    numIndexTuples=0.0
    spc_random_page_cost=0.0
    num_sa_scans=0.0


# Btree : function btcostestimate in src/backend/utils/adt/selfuncs.c
# 计算索引选择性，
# 对于这个主键扫描的case，直接返回本身
def calindexSelectivity(b):
    return b # ignore


# 估算需要从磁盘读取的pages
# paper Index Scans Using a Finite LRU Buffer: A Validated I/O Model
# The Mackert and Lohman approximation is that the number of pages
# fetched is
#	PF =
#		min(2TNs/(2T+Ns), T)			when T <= b
#		2TNs/(2T+Ns)					when T > b and Ns <= 2Tb/(2T-b)
#		b + (Ns - 2Tb/(2T-b))*(T-b)/T	when T > b and Ns > 2Tb/(2T-b)
# where
#		T = # pages in table
#		N = # tuples in table
#		s = selectivity = fraction of table to be scanned
#		b = # buffer pages available (we include kernel space here)
def index_pages_fetched(tuples_fetched, pages, index_pages):
    T=(pages * 1.0) if (pages > 1) else 1.0
    total_pages=pages + index_pages
    pages_fetched=1
    b=math.ceil(effective_cache_size * T / total_pages)
    if T <= b:
        pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched)
        if (pages_fetched >= T):
            pages_fetched = T
    else:
        lim=0.0
        lim=(2.0 * T * b) / (2.0 * T - b)
        if tuples_fetched <= lim:
            pages_fetched=(2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched)
        else:
            pages_fetched=b + (tuples_fetched - lim) * (T - b) / T
        pages_fetched=math.ceil(pages_fetched)

    return pages_fetched



## 
## function genericcostestimate in src/backend/utils/adt/selfuncs.c
def genericcostestimate(costs):
    num_sa_scans=1
    numIndexTuples=costs.numIndexTuples
    # 当前条件下相关的pages = btreeSelectivity * index_pages
    numIndexPages = math.ceil(numIndexTuples * index_pages / index_tuples)
    # 读取的pages的成本
    indexTotalCost = numIndexPages * random_page_cost
    # 操作tuple的成本
    indexTotalCost += numIndexTuples * num_sa_scans * (cpu_index_tuple_cost + qual_op_cost)
    # 
    indexSelectivity=calindexSelectivity(btreeSelectivity)
    costs.indexTotalCost = indexTotalCost
    costs.indexSelectivity = indexSelectivity
    costs.numIndexPages = numIndexPages
    costs.numIndexTuples = numIndexTuples
    costs.spc_random_page_cost = random_page_cost




def btcostestimate():
    costs = GenericCosts()
    ## 这个case只有一个索引条件，直接等于1, 可以认为是遍历btree的次数
    num_sa_scans = 1

    ## index tuples
    numIndexTuples = btreeSelectivity * index_tuples
    costs.numIndexTuples = numIndexTuples

    genericcostestimate(costs)

    # btree向下遍历的tuples成本
    descentCost = math.ceil(math.log2(index_tuples)) * cpu_operator_cost
    costs.indexStartupCost = descentCost
    costs.indexTotalCost += num_sa_scans * descentCost

    # btree向下遍历的pages成本 = （树高 + 1）* 50 * cpu_operator_cost
    descentCost = (index_tree_height + 1) * 50.0 * cpu_operator_cost
    costs.indexStartupCost += descentCost
    costs.indexTotalCost += num_sa_scans * descentCost
    return costs

# src/backend/optimizer/path/costsize.c
# cost_index
def cost_index():
    costs=btcostestimate()

    ## /* all costs for touching index itself included here */
    startup_cost = costs.indexStartupCost
    run_cost = costs.indexTotalCost - costs.indexStartupCost

    ## /* estimate number of main-table tuples fetched */
    tuples_fetched = (costs.indexSelectivity * baserel_tuples)

    pages_fetched=0
    max_io_cost=0 
    min_io_cost=0

    loop_count=1
    if loop_count==1:
        pages_fetched=index_pages_fetched(tuples_fetched,baserel_pages,index_pages)

        # 最坏情况下：所有page都是随机读取
        max_io_cost = pages_fetched * random_page_cost
        pages_fetched=math.ceil(costs.indexSelectivity * baserel_pages)

        # 最优情况下：第一个page是随机cost + 后面的是顺序cost
        min_io_cost = 1*random_page_cost + (pages_fetched - 1) * seq_page_cost


    csquared = indexCorrelation * indexCorrelation
    run_cost += max_io_cost + csquared * (min_io_cost - max_io_cost)

    # 处理所有tuples的cost
    cpu_run_cost = cpu_per_tuple * tuples_fetched

    # run_cost = 索引成本 + 主表成本
    # run_cost = costs.indexTotalCost - costs.indexStartupCost + 
    #            cpu_run_cost + max_io_cost + csquared * (min_io_cost - max_io_cost)
    run_cost += cpu_run_cost


    print("startup_cost: ", startup_cost)
    print("indexTotalCost: ", costs.indexTotalCost)
    print("min_io_cost: ", min_io_cost)
    print("max_io_cost: ", max_io_cost)
    print("run_cost: ", run_cost)
    print("total_cost: ", run_cost+startup_cost)

cost_index()



