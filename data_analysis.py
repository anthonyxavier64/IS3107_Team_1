import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from kneed import KneeLocator
from sklearn import preprocessing
from scipy.spatial.distance import cdist
from sklearn.cluster import AgglomerativeClustering
import plotly
import plotly.express as px

#obtain data
df = pd.read_csv('test.csv')
# print(df.head())

#obtain audio features mean
def get_audio_feature_mean():
    audiofeat_df = df[['danceability', 'energy', 'speechiness', 'acousticness', 'liveness', 'valence']]
    plt.figure(figsize=(10,4))
    audiofeat_df.mean().plot.bar()
    plt.title('Mean Values of Audio Features')
    plt.savefig('vis_imgs/audio_feature_mean')

def get_feature_hist():
    # fetching a list of song features
    features = ['danceability', 'energy', 'key', 'loudness' ,'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence','tempo']

    # plotting histogram for each feature
    for col in features:
        sns.displot(df, x=col, kde=True)
        plt.savefig(f'vis_imgs/{col}.png')
    
def get_heatmap():
    # Generate a mask for the upper triangle
    mask = np.triu(np.ones_like(df.corr(), dtype=bool))

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 8))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(230, 20, as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(df.corr(), mask=mask, cmap=cmap, vmin=0, vmax=.5, 
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    
    plt.savefig('vis_imgs/heatmap')

#key mode timesignature removed to not analyse categorical factors; duration_ms and tempo removed due to multicollinearity 
clusterdf = df.drop(columns=['type', 'uri', 'id', 'track_href','analysis_url','key','mode','time_signature','duration_ms','tempo'])

colnames = ['danceability','energy','loudness','speechiness','acousticness','instrumentalness','liveness','valence']

# scale the data for better results
clusterdf_scaled_arr = preprocessing.scale(clusterdf)
clusterdf_scaled = pd.DataFrame(clusterdf_scaled_arr, columns = colnames)

def get_cluster_n():
    clusters=range(1,15)
    meandist=[]

    for k in clusters:
        model=KMeans(n_clusters=k)
        model.fit(clusterdf_scaled)
        clusassign=model.predict(clusterdf_scaled)
        meandist.append(sum(np.min(cdist(clusterdf_scaled, model.cluster_centers_, 'euclidean'), axis=1))
        / clusterdf_scaled.shape[0])

    plt.plot(clusters, meandist)
    plt.xlabel('Number of clusters')
    plt.ylabel('Average distance')
    # plt.show()

    kl = KneeLocator(range(1, 15), meandist, curve="convex", direction="decreasing")
    return(kl.elbow)

n_clust = get_cluster_n()

def get_cluster_chart(n_clust):
    ac_model = AgglomerativeClustering(n_clusters=n_clust, affinity='euclidean', linkage='ward')  
    cluster = ac_model.fit_predict(clusterdf_scaled)
    clusterdf['cluster'] = cluster

    polar = clusterdf.groupby("cluster").mean().reset_index()
    polar = pd.melt(polar,id_vars=["cluster"])
    polarchart = px.line_polar(polar, r="value", theta="variable", color="cluster", line_close=True,height=800,width=1400)
    plotly.offline.plot(polarchart, filename='vis_imgs/polarcluster.html')

    clust_means_wt = pd.DataFrame(clusterdf.groupby('cluster').mean(), columns=clusterdf.columns)
    sns.heatmap(clust_means_wt.T,  linewidths=.5, cmap="YlGnBu")
    plt.savefig('heatmapcluster.png')

get_cluster_chart(n_clust)

