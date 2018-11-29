## NOTE run from project dir `PYTHONPATH=. -s tests`

import pytest
import os

from hurlers.game import Game
from yesterdays_pitchers import build_pitch_dict 

@pytest.fixture(scope="session")
def game():
    xml_dir = "/Users/houston/_devel/projects/pitches-pipeline/games_xml_2018"
    g = Game(os.path.join(xml_dir, "gid_2018_10_28_bosmlb_lanmlb_1"))
    yield g
    
    
def test_build_pitchers(game):
    assert game != None 
    pitchers = build_pitch_dict(
        reduce(lambda x,y: x+y, [g.get_pitchers() for g in [game]])
    )
    pitcher_ids = set([
        '523260',
        '445276',
        '477132',
        '519242',
        '456034', 
        '520980'
    ])
    assert len(pitchers) == len(pitcher_ids)
    for pitcher_id in pitchers.keys():
        assert pitcher_id in pitchers 
