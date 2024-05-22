#!/usr/bin/env python

from pyraft import naive_raft_v2

node = naive_raft_v2.make_default_node()

node.start()
node.join()



