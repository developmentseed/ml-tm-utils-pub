"""
Tests for utility functions
"""

import os
from os import path as op
import unittest
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pygeotile.tile import Tile

from ml_tm_utils_pub.utils_geodata import (get_tile_pyramid, _get_md5_checksum,
                                    _test_geoj_equality,
                                    get_stripped_geojson_tasks,
                                    _get_quadrant_tiles)
from ml_tm_utils_pub.utils_database import (Project, TilePredBA,
                                     update_db_project,
                                     get_total_tiles_building_area,
                                     Base)

testpath = os.path.dirname(__file__)
fpath_geojson = op.join(testpath, 'mini_tm_project.geojson')


class GeoTest(unittest.TestCase):
    """Test geodata utility functionality"""

    def test_finding_child_tiles(self):
        """Check if 4 correct child tiles can be found from a parent tile."""

        parent_tile = Tile.from_tms(1412, 3520, 17)
        ground_truth = [Tile.from_tms(x, y, z) for x, y, z in
                        [(2825, 7041, 18), (2825, 7040, 18),
                         (2824, 7041, 18), (2824, 7040, 18)]]
        children_tiles = _get_quadrant_tiles(parent_tile)
        self.assertCountEqual(ground_truth, children_tiles)

    def test_pyramid_of_child_indices(self):
        """Check if a set children tiles can be found from a parent tile."""

        tile_dict = dict(x=5, y=314, z=16)
        children_tiles = get_tile_pyramid(tile_dict)
        ground_truth = ['18-23-1259', '18-23-1258', '18-22-1259', '18-22-1258',
                        '18-23-1257', '18-23-1256', '18-22-1257', '18-22-1256',
                        '18-21-1259', '18-21-1258', '18-20-1259', '18-20-1258',
                        '18-21-1257', '18-21-1256', '18-20-1257', '18-20-1256']

        # Misleading name, but checks that unordered list matches
        self.assertCountEqual(children_tiles, ground_truth)

    def test_geojson_stripping(self):
        """Check remove of non-geo information from a geojson file."""

        ground_truth = ('{"taskID": 338, "geometry": {"type": "MultiPolygon", "coordinates": '
                        '[[[[106.74591062541, 10.857583692634], [106.74591062541, '
                        '10.8602810943596], [106.748657207441, 10.8602810943596], '
                        '[106.748657207441, 10.857583692634], [106.74591062541, '
                        '10.857583692634]]]]}}')

        # Load geojson
        with open(fpath_geojson, 'r') as f:
            text_data = f.read()
        stripped_geojson = get_stripped_geojson_tasks(text_data)

        # Test that stripped geojson matches expectations
        self.assertMultiLineEqual(ground_truth, stripped_geojson)

        # Test that MD5 checksum of geojson matches expectations
        self.assertEqual(_get_md5_checksum(ground_truth),
                         _get_md5_checksum(stripped_geojson))

        # Test helper to both strip and hash geojson can do absolute basics
        self.assertTrue(_test_geoj_equality(text_data, text_data))

        json_dict = json.loads(text_data)
        json_dict['tasks']['features'][0]['properties']['taskId'] = 339

        changed_text_data = get_stripped_geojson_tasks(json.dumps(json_dict))
        self.assertNotEqual(changed_text_data, stripped_geojson)


class DatabaseTest(unittest.TestCase):
    """Test database utility functionality."""

    def test_database_utils_integration(self):
        """Check multiple utilities concerning database manipulations."""
        print('Testing creation of project and tile predictions')

        # Set `echo` to True for verbose
        engine = create_engine('sqlite:///:memory:', echo=False)
        Base.metadata.create_all(engine)

        # Create session maker
        Session = sessionmaker(bind=engine)  # OK to create w/out engine
        session = Session()

        proj_1 = Project(tm_index=26, json_geometry='', md5_hash='')
        session.add(proj_1)

        tile_pred_1 = TilePredBA(tile_index='18-1241-23141',
                                 building_area_ml=5.9, building_area_osm=10.,
                                 project=proj_1)
        session.add(tile_pred_1)
        session.add_all([TilePredBA(tile_index='18-2825-7041',
                                    building_area_ml=0, building_area_osm=1.,
                                    project=proj_1),
                         TilePredBA(tile_index='18-2824-7041',
                                    building_area_ml=0.99,
                                    building_area_osm=5.1,
                                    project=proj_1),
                         TilePredBA(tile_index='18-2824-7040',
                                    building_area_ml=0, building_area_osm=0.,
                                    project=proj_1),
                         TilePredBA(tile_index='18-2825-7040',
                                    building_area_ml=99.01,
                                    building_area_osm=0.9,
                                    project=proj_1)])
        session.commit()

        tile = session.query(TilePredBA).join(Project).filter(
            Project.id == proj_1.id).first()
        self.assertEqual(tile.tile_index, '18-1241-23141')
        self.assertEqual(tile.building_area_ml, 5.9)
        self.assertEqual(tile.building_area_osm, 10.)

        ###################################
        print('Testing geojson manipulations')
        fname_geojson = './mini_tm_project.geojson'

        # Load geojson
        with open(fname_geojson, 'r') as f:
            text_data = f.read()

        # Strip/hash geojson
        stripped_geojson = get_stripped_geojson_tasks(text_data)
        json_hash = _get_md5_checksum(stripped_geojson)
        update_db_project(proj_1.tm_index, stripped_geojson, json_hash,
                          session)

        self.assertEqual(json_hash, '59c6615d20a2067c4362a44b26e54e0c')

        ###################################
        print('Testing ML predicted and OSM area calculations')
        tile_dict = dict(x=1412, y=3520, z=17)
        children_tiles = get_tile_pyramid(tile_dict)

        area_ml, area_osm = get_total_tiles_building_area(children_tiles,
                                                          session)

        self.assertEqual(area_ml, 100.)
        self.assertEqual(area_osm, 7.)

        ###################################
        # Test project changes
        print('Testing project geometry changes')

        project = session.query(Project).filter(Project.tm_index == 26).one()
        project.json_geometry = stripped_geojson
        orig_hash = _get_md5_checksum(stripped_geojson)
        self.assertEqual(project.md5_hash, orig_hash)

        # Change project to bad geojson string, check for breaks
        new_proj_geojson = 'bad_geojson_str'
        update_db_project(26, new_proj_geojson,
                          _get_md5_checksum(new_proj_geojson), session)
        new_project = session.query(Project).filter(Project.tm_index == 26).one()
        new_hash = new_project.md5_hash
        self.assertNotEqual(new_hash, orig_hash)
