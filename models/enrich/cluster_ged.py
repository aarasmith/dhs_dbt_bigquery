import pandas as pd
import geopandas as gpd

def model(dbt, fal):
    ged = dbt.ref('ged_lat_long')
    clusters = dbt.ref('unique_clusters')

    clusters = gpd.GeoDataFrame(
        clusters,
        geometry=gpd.points_from_xy(clusters.long, clusters.lat),
        crs="EPSG:4326"
    )
    clusters.to_crs(crs="+proj=eck6 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs", inplace=True)

    ged = gpd.GeoDataFrame(
        ged,
        geometry=gpd.points_from_xy(ged.longitude, ged.latitude),
        crs="EPSG:4326"
    )
    clusters_out = []
    for buffer_length in [25000, 50000, 100000]:
        clusters_buffer = clusters.copy()
        clusters_buffer.geometry = clusters_buffer.geometry.buffer(buffer_length)
        clusters_buffer.to_crs(crs="EPSG:4326", inplace=True)

        cluster_ged = clusters_buffer.sjoin(ged, how="left")
        cluster_ged['dist'] = buffer_length / 1000
        cluster_ged = pd.DataFrame(cluster_ged.drop(columns=['lat', 'long', 'geometry', 'index_right', 'latitude', 'longitude']))
        
        clusters_out.append(cluster_ged)

    return pd.concat(clusters_out)

