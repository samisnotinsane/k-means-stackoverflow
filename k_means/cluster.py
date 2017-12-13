import numpy as np
import matplotlib.pyplot as plt


class KMeans():

    def euclidian(a, b):
        return np.linalg.norm(a - b)

    # Trains an unsupervised model using K-Means.
    def train(dataset, k, epsilon=0):
        history_centroids = []
        dataset = np.loadtxt('durudataset.txt')
        # dataset = dataset[:, 0:dataset.shape[1] - 1]
        num_instances, num_features = dataset.map(lambda x,y:  x.shape)
        prototypes = dataset[np.random.randint(0, num_instances - 1, size=k)]
        history_centroids.append(prototypes)
        prototypes_old = np.zeros(prototypes.shape)
        belongs_to = np.zeros((num_instances, 1))
        norm = dist_method(prototypes, prototypes_old)
        iteration = 0
        while norm > epsilon:
            iteration += 1
            norm = dist_method(prototypes, prototypes_old)
            prototypes_old = prototypes
            for index_instance, instance in enumerate(dataset):
                dist_vec = np.zeros((k, 1))
                for index_prototype, prototype in enumerate(prototypes):
                    dist_vec[index_prototype] = dist_method(prototype,
                                                            instance)

                belongs_to[index_instance, 0] = np.argmin(dist_vec)

            tmp_prototypes = np.zeros((k, num_features))

        for index in range(len(prototypes)):
            instances_close = [i for i in range(
                len(belongs_to)) if belongs_to[i] == index]
            prototype = np.mean(dataset[instances_close], axis=0)
            # prototype = dataset[np.random.randint(0, num_instances, size=1)[0]]
            tmp_prototypes[index, :] = prototype

        prototypes = tmp_prototypes

        history_centroids.append(tmp_prototypes)

        # plot(dataset, history_centroids, belongs_to)

        return prototypes, history_centroids, belongs_to


class Graph():
    def plot(dataset, centroid_history, point_owners):
        pass
