# # This function combines adds elements and returns them as the first element
# # of the tuple. Then increments the second value by one in each partition.
# seqOp = (lambda acc, value: (acc[0] + value, acc[1] + 1))
# # This function takes all the partitions and combines them.
# combOp = (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
# # Aggregate and get a tuple back (total, count)
# level_agg = sc.parallelize(labels).aggregate((0,0),seqOp,combOp)
