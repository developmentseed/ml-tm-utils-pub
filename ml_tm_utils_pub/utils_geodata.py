"""
Utility functions for manipulating geospatial data like geojsons, tile
indicies, cloud-optimized geotiffs, etc.
"""

import csv
import hashlib
import json
import ast
from queue import LifoQueue

import numpy as np
from pygeotile.tile import Tile
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio import crs

from pyproj import Proj, transform


def read_csv_building_area_preds(fpath_csv):
    """Convert 2 column CSV into key/val pairs

    Parameters
    ----------
    fpath_csv: str
        Filepath to CSV file with tile indices and building areas

    Returns
    -------
    building_areas: dict
        Dictionary with tile indices as keys and float values as vals
    """

    building_areas = {}
    with open(fpath_csv, newline='') as csv_file:
        file_reader = csv.reader(csv_file)
        for row in file_reader:
            print(row)
            k1, k2, k3 = ast.literal_eval(row[0])
            tile_ind = '{}-{}-{}'.format(k1, k2, k3)
            building_areas[tile_ind] = float(row[1])

    return building_areas


def get_stripped_geojson_tasks(text_data):
    """Return a list of task strings with only task ID and geometry.

    Parameters
    ----------
    text_data: str
        String with json formatting representing a TM project

    Returns
    -------
    stripped_tasks: str
        JSON string containing only sorted tasks and their geometry.
    """

    # Strip everything except geometry
    json_dict = json.loads(text_data)

    if not "tasks" in json_dict.keys():
        raise ValueError('Loaded geojson missing "tasks".')

    stripped_tasks = []
    for task in json_dict['tasks']['features']:
        stripped_tasks.append('{{"taskID": {}, "geometry": {}}}'.format(
            str(task['properties']['taskId']).replace("'", '"'),
            str(task['geometry']).replace("'", '"')))

    # Return concatenated version of sorted list. Sort necessary to prevent
    #     simple changes in order from affecting downstream hashing
    return '\n'.join(sorted(stripped_tasks))


def _get_md5_checksum(str_obj):
    """Return digested MD5 checksum for string"""

    # Encode unicode as utf-8 if needed; this is necessary for hashing
    if isinstance(str_obj, str):
        str_obj = str_obj.encode('utf-8')

    return hashlib.md5(str_obj).hexdigest()


def _test_geoj_equality(geojson_str_1, geojson_str_2):
    """Test if *geometry* for two geojsons are identical."""
    h1 = _get_md5_checksum(get_stripped_geojson_tasks(geojson_str_1))
    h2 = _get_md5_checksum(get_stripped_geojson_tasks(geojson_str_2))

    return h1 == h2


def _get_quadrant_tiles(tile):
    """Return indicies of tiles at one higher zoom (in TMS tiling scheme)"""
    ul = (tile.tms[0] * 2, tile.tms[1] * 2)

    return [Tile.from_tms(ul[0], ul[1], tile.zoom + 1),           # UL
            Tile.from_tms(ul[0], ul[1] + 1, tile.zoom + 1),       # LL
            Tile.from_tms(ul[0] + 1, ul[1], tile.zoom + 1),       # UR
            Tile.from_tms(ul[0] + 1, ul[1] + 1, tile.zoom + 1)]   # LR


def get_tile_pyramid(top_tile_dict, max_zoom=18, ret_format='{z}-{x}-{y}'):
    """Get all children of a tile at a specific zoom.

    Parameters:
    ----------
    top_tile_dict: dict
        Tile for which to get children down to some zoom level. 'x', 'y', 'z'
        should be defined keys corresponding to TMS coordinates.
    max_zoom: int
        Zoom at which to terminate file search
    ret_format: str
        Return format for strings

    Returns:
    -------
    tile_inds: list of str
        All tiles at the specified zoom that underly the top tile
    """

    # Initialize queue and add input tile
    stack = LifoQueue()
    stack.put(Tile.from_tms(top_tile_dict['x'],
                            top_tile_dict['y'],
                            top_tile_dict['z']))

    # Depth-first search on tile indices
    desired_tiles = []
    while not stack.empty():

        # Pop the top tile in the stack
        temp_tile = stack.get()

        # Check if desired zoom has been reached
        if temp_tile.zoom >= max_zoom:
            # If at max zoom, save tile
            tile_dict = dict(x=temp_tile.tms[0],
                             y=temp_tile.tms[1],
                             z=temp_tile.zoom)
            desired_tiles.append(ret_format.format(**tile_dict))

        # Otherwise, zoom in one increment, find children tiles, add to stack
        else:
            for rt in _get_quadrant_tiles(temp_tile):
                stack.put(rt)

    return desired_tiles


def cog_windowed_read(image_path, tile_ind, chan_inds=(1,), final_proj=None):
    """Get raster data from a cloud-optimized-geotiff using a tile's bounds.

    Parameters
    ----------
    image_path: str
        COG file path as local file or path to remote image.
    tile_ind: dict or str
        Dictionary with keys `z`, `x`, `y` defined or str in `z-x-y` format.
    chan_inds: tuple of int
        Channel indicies to grab from COG. Usually, `(1)` for L and `(1, 2, 3)`
        for RGB.
    final_proj: str
        Output projection for data if a projection different from the COG is
        needed.

    Returns
    -------
    window_data: np.ndarray
        Array containing data values requested in tile_ind.
    """

    if isinstance(tile_ind, dict):
        tile = Tile.from_tms(tile_ind['x'], tile_ind['y'], tile_ind['z'])
    elif isinstance(tile_ind, str):
        z, x, y = [int(val) for val in tile_ind.split('-')]
        tile = Tile.from_tms(x, y, z)
    else:
        raise ValueError('Could not parse `tile_ind` as string or dict: {}'.format(tile_ind))

    with rasterio.open(image_path) as cog_image:

        p1 = Proj({'init': 'epsg:4326'})
        p2 = Proj(**cog_image.crs)

        # Convert tile lat/lon bounds to COG ref frame
        #   (pygeotile bounds are (LL, UR))
        window_bounds = dict()
        window_bounds['west'], window_bounds['north'] = \
            transform(p1, p2, tile.bounds[0].longitude, tile.bounds[1].latitude)
        window_bounds['east'], window_bounds['south'] = \
            transform(p1, p2, tile.bounds[1].longitude, tile.bounds[0].latitude)

        # Get image origin point and resolution from the COG
        tif_bounds = dict(north=cog_image.bounds.top,
                          west=cog_image.bounds.left)
        x_res, y_res = cog_image.transform[0], cog_image.transform[4]

        # Calculate the pixel indices of the window
        top = int((window_bounds['north'] - tif_bounds['north']) / y_res)
        left = int((window_bounds['west'] - tif_bounds['west']) / x_res)
        bottom = int((window_bounds['south'] - tif_bounds['north']) / y_res)
        right = int((window_bounds['east'] - tif_bounds['west']) / x_res)

        window_pixels = ((top, bottom), (left, right))

        # Access the pixels of TIF image
        # XXX Seems to force data to the output shape
        window_data = np.empty((len(chan_inds), 256, 256),
                               cog_image.profile['dtype'])
        for ci in chan_inds:
            cog_image.read(ci, window=window_pixels, out=window_data[ci - 1],
                           boundless=True)

        # If user wants a specific transform, do that now
        if final_proj is not None:
            profile = cog_image.profile
            dst_crs = crs.from_string(final_proj)

            # Calculate the ideal dimensions and transformation in the new crs
            # XXX Possible to define resolution here
            dst_affine, dst_width, dst_height = calculate_default_transform(
                cog_image.crs, dst_crs, 256, 256, left=left, bottom=bottom,
                right=right, top=top)

            profile.update({'crs': dst_crs, 'transform': dst_affine,
                            'affine': dst_affine, 'width': dst_width,
                            'height': dst_height})

            # Create an array for the projected window
            window_data_proj = np.empty((len(chan_inds), dst_height, dst_width),
                                        cog_image.profile['dtype'])
            reproject(source=window_data, src_crs=cog_image.crs,
                      src_transform=cog_image.affine,
                      destination=window_data_proj,
                      dst_transform=dst_affine,
                      dst_crs=dst_crs,
                      resampling=Resampling.nearest)

            return np.moveaxis(window_data_proj, 0, -1)

        return np.moveaxis(window_data, 0, -1)


def get_pixel_area(latitude, zoom):
    """Calculate the area per pixel in a tile for a given latitude and zoom.

    Parameters
    ----------
    latitude: float
        Latitude in degrees. Should be on interval [-90, 90]
    zoom: int
        OSM zoom level. Should be on interval [0, 19]

    Returns
    ----------
    area: float
        Area per pixel in square meters

    Notes: equation from:
        https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Resolution_and_Scale
    """

    # Error checking
    if latitude < -90 or latitude > 90:
        raise ValueError('latitude of {} outside bounds of [-90, 90]'.format(latitude))
    if not isinstance(zoom, int):
        raise ValueError('zoom must be an `int`, got {}'.format(type(zoom)))
    if zoom < 0 or zoom > 19:
        raise ValueError('zoom of {} outside bounds of [0, 19]'.format(zoom))

    pix_width = 156543.03 * np.cos(np.deg2rad(latitude)) / (2 ** zoom)

    # Return area of pixel
    return pix_width ** 2
