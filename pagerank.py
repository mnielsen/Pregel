"""pagerank.py illustrates how to use the pregel.py library, and tests
that the library works.

It illustrates pregel.py by computing the PageRank for a randomly
chosen 10-vertex web graph.

It tests pregel.py by computing the PageRank for the same graph in a
different, more conventional way, and showing that the two outputs are
near-identical."""

from pregel import *

# The next two imports are only needed for the test.  
from numpy import * 
import random

num_workers = 4
num_vertices = 10

def main():
    vertices = [PageRankVertex(j,1.0/num_vertices,[]) 
                for j in range(num_vertices)]
    create_edges(vertices)
    pr_test = pagerank_test(vertices)
    print "Test computation of pagerank:\n%s" % pr_test
    pr_pregel = pagerank_pregel(vertices)
    print "Pregel computation of pagerank:\n%s" % pr_pregel
    diff = pr_pregel-pr_test
    print "Difference between the two pagerank vectors:\n%s" % diff
    print "The norm of the difference is: %s" % linalg.norm(diff)

def create_edges(vertices):
    """Generates 4 randomly chosen outgoing edges from each vertex in
    vertices."""
    for vertex in vertices:
        vertex.out_vertices = random.sample(vertices,4)

def pagerank_test(vertices):
    """Computes the pagerank vector associated to vertices, using a
    standard matrix-theoretic approach to computing pagerank.  This is
    used as a basis for comparison."""
    I = mat(eye(num_vertices))
    G = zeros((num_vertices,num_vertices))
    for vertex in vertices:
        num_out_vertices = len(vertex.out_vertices)
        for out_vertex in vertex.out_vertices:
            G[out_vertex.id,vertex.id] = 1.0/num_out_vertices
    P = (1.0/num_vertices)*mat(ones((num_vertices,1)))
    return 0.15*((I-0.85*G).I)*P

def pagerank_pregel(vertices):
    """Computes the pagerank vector associated to vertices, using
    Pregel."""
    p = Pregel(vertices,num_workers)
    p.run()
    return mat([vertex.value for vertex in p.vertices]).transpose()

class PageRankVertex(Vertex):

    def update(self):
        # This routine has a bug when there are pages with no outgoing
        # links (never the case for our tests).  This problem can be
        # solved by introducing Aggregators into the Pregel framework,
        # but as an initial demonstration this works fine.
        if self.superstep < 50:
            self.value = 0.15 / num_vertices + 0.85*sum(
                [pagerank for (vertex,pagerank) in self.incoming_messages])
            outgoing_pagerank = self.value / len(self.out_vertices)
            self.outgoing_messages = [(vertex,outgoing_pagerank) 
                                      for vertex in self.out_vertices]
        else:
            self.active = False

if __name__ == "main":
    main()
