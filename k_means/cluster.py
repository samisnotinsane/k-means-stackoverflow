from random import *
"""
" Implementation of K-Means clustering algorithm
" based on Andrew NG's lecture from Stanford University.
" https://www.coursera.org/learn/machine-learning/lecture/93VPG/k-means-algorithm
"""
class Cluster():

    def euclidian_distance(x, mu):
        return x - mu

    def minimise(x, mu):
        return euclidian_distance(x[i], mu[k])

    """
    " x = unlabelled training set.
    """
    def k_means(K, x):
        
        if K < 0:
            raise ValueError('[AGENT] k must be non-negative')
        
        # Initialise K centroids randomly across training set.
        mu = sample(x, K)
        m = len(training_set)
        k = 1
        mu_prev[k] = mu[k] 
        while(mu[k] != mu_prev[k]):
            # Cluster assignment step.
            for i=1 to m:
                # Compute norm and minimise for k.
                c[i] = minimise(x[i], mu[k])

            # Move centroid step
            # for k to K:
                
            k++
        

        return -1
