import json
import os
import pkg_resources
import numpy as np
from scipy.spatial import voronoi_plot_2d
from scipy.spatial.qhull import Voronoi
from shapely.geometry import Polygon, Point
# from scipy.spatial import Voronoi, voronoi_plot_2d
import pandas as pd
import geopandas as gpd
import shapely.geometry
import shapely.ops
import matplotlib.pyplot as plt
import copy
# from voronoi.algorithm import Algorithm as Voronoi
# from voronoi.graph.polygon import Polygon
# from voronoi.graph.point import Point


def get_resource_path(resource):
    res = pkg_resources.resource_filename(__name__, resource)
    if os.path.exists(res):
        return res
    else:
        raise UnableFindResource(resource)


class UnableFindResource(Exception):
    def __init__(self, res):
        Exception.__init__(self, 'Unable to find %s' % res)


def _voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.

    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.

    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of inputs vertices, with 'points at infinity' appended to the
        end.

    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram

    """

    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D inputs")

    new_regions = []
    new_vertices = vor.vertices.tolist()

    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()

    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))

    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]

        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue

        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue

            # Compute the missing endpoint of an infinite ridge

            t = vor.points[p2] - vor.points[p1]  # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius

            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())

        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]

        # finish
        new_regions.append(new_region.tolist())

    return new_regions, np.asarray(new_vertices)


def get_voronoi_polygons(points_dict):
    # points = np.array(list(points_dict.values()))[:, :2]
    points_list = [[79.95818, 6.865576], [79.941176, 6.923571]] #gives error
    #points_list = [['79.95818', '6.865576'], ['79.919','6.908'], ['79.885825', '6.860887']] #working fine
    points = np.array(points_list)

    print(points)
    vor = Voronoi(points)
    voronoi_plot_2d(vor)
    plt.show()

def get_voronoi_polygonss(points_dict):
    # points = [
    #     (2.5, 2.5),
    #     (4, 7.5),
    #     (7.5, 2.5),
    #     (6, 7.5),
    #     (4, 4),
    #     (3, 3),
    #     (6, 3),
    # ]

    points = [(79.95818, 6.865576), (79.941176, 6.923571)]

    # Define a bounding box
    # polygon = Polygon([
    #     (2.5, 10),
    #     (5, 10),
    #     (10, 5),
    #     (10, 2.5),
    #     (5, 0),
    #     (2.5, 0),
    #     (0, 2.5),
    #     (0, 5),
    # ])

    polygon = Polygon([(79, 6), (80, 7)])

    # Initialize the algorithm
    v = Voronoi(polygon)

    # Create the diagram
    v.create_diagram(points=points, vis_steps=False, verbose=False, vis_result=True, vis_tree=True)

    # Get properties
    edges = v.edges
    vertices = v.vertices
    arcs = v.arcs
    points = v.points


if __name__ == "__main__":
    kelani_basin_points_file = get_resource_path('extraction/local/kelani_basin_points_250m.txt')
    kelani_lower_basin_shp_file = get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
    reference_net_cdf = get_resource_path('extraction/netcdf/wrf_wrfout_d03_2019-03-31_18_00_00_rf')
    points = np.genfromtxt(kelani_basin_points_file, delimiter=',')
    config_path = os.path.join(os.getcwd(), 'config.json')
    with open(config_path) as json_file:
        config = json.load(json_file)
        if 'klb_obs_stations' in config:
            obs_stations = copy.deepcopy(config['klb_obs_stations'])
        kel_lon_min = np.min(points, 0)[1]
        kel_lat_min = np.min(points, 0)[2]
        kel_lon_max = np.max(points, 0)[1]
        kel_lat_max = np.max(points, 0)[2]

        print('[kel_lon_min, kel_lat_min, kel_lon_max, kel_lat_max] : ',
              [kel_lon_min, kel_lat_min, kel_lon_max, kel_lat_max])
        print('obs_stations : ', obs_stations)
        get_voronoi_polygons(obs_stations)
