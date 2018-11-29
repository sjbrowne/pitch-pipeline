from __future__ import print_function

import os
import sys
import datetime
import json
import csv
import multiprocessing

import pandas as pd
import psycopg2 as psy
import click

from hurlers.utils.soup_utils import GET_page_soup
from hurlers.utils.url_utils import *
from hurlers.utils.csv_utils import pitches_to_csv
from hurlers.game import Game
from hurlers.player import Player
from hurlers.pitch import Pitch

from scripts.stats import write_award, write_pitcher_stats

YESTERDAY = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
AWARD_ATTRS = set([
    "start_speed",
    "pfx_x",
    "pfx_z",
    "spin_rate",
    "break_angle"
])

def build_pitcher_dict(pitchers):
    P = {}
    for p in pitchers:
      P[p.id] = p.get_formatted_name()
    return P

def _get_postgres_con(config_filename):
    with open(config_filename) as f:
        config = json.load(f)

        dbname = config['database']['dbname']
        user = config['database']['user']
        password = config['database']['password']
        dbparams = "dbname={dbname} user={user} password={password}".format(
            dbname=dbname, user=user, password=password)

        try:
            return psy.connect(dbparams) 
        except psy.OperationalError as e:
            print("[ERR] could not establish database connection, no tuples written.",
            file=sys.stderr)

def _insert_pitches_into_postgres(csv_filename, table, con):
    with open(csv_filename) as f:
        rdr = csv.DictReader(f)
        cur = con.cursor()

        fields = sorted(rdr.fieldnames)
        fields.append('pitch_id')
        fstr = ', '.join(fields)
        v = ', '.join(['%s' for _ in range(len(fields))])

        query = """
        INSERT into {} ({})
        VALUES ({})
        ON CONFLICT DO NOTHING
        """.format(table, fstr, v)

        for row in rdr:
            pitch_id = Pitch.create_pitch_id(
              row['tfs_zulu'],
              row['pitcher'],
              row['id'])
            values = map(lambda x: None if not x else x,
              [ row[x] for x in sorted(rdr.fieldnames)])
            values.append(pitch_id)
            cur.execute(query, tuple(values))
        con.commit()

def _insert_players_into_postgres(players, table, con):
    query = """
    INSERT into {} VALUES (?, ?, ?)
    ON CONFLICT DO NOTHING;
    """.format(table)

    for player in players:   
        cur.execute(sql, (int(player.id), player.first, player.last))
    con.commit()

        


def _map_to_file_if_exists(gid_url, xml_dir):
    gid_dir = os.path.join(xml_dir, url_to_gid(gid_url))
    if os.path.isdir(gid_dir):
        return gid_dir
    return gid_url

def _create_filename_from_date(path, prefix, day, suffix=""):
   if not os.path.isdir(path):
       os.makedirs(path) 
   return os.path.join(path, prefix+day.strftime("%Y_%m_%d")+suffix)  

@click.command()
@click.option("-x", default="",          help="Where to store xml.")
@click.option("-c", default="",          help="Where to store csv.")
@click.option("-d", default=YESTERDAY,   help="Game date.")
@click.option("-p", default=False,       help="Save pitches to db. (Requires configuration).", is_flag=True )
@click.option("-y", default=False,       help="Save players to db. (Requires configuration).", is_flag=True )
@click.option("-a", default="",          help="Where to store awards." )
@click.option("-s", default="",          help="Where to strore stat summary.")
@click.pass_context
def process_pitches(ctx, x, c, d, p, y, a, s): 
   yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
   cwd = os.getcwd()
   day = datetime.datetime.strptime(d, "%Y-%m-%d") 

   xml_dir = os.path.join(cwd, x) if not os.path.isabs(x) else x if x else cwd
   csv_dir = os.path.join(cwd, c) if not os.path.isabs(c) else c if c else cwd
   awards_dir = os.path.join(cwd, a) if not os.path.isabs(a) else a if a else cwd
   stats_dir = os.path.join(cwd, s) if not os.path.isabs(s) else s if s else cwd

   csv_prefix, csv_suffix = "pitches__", ".csv"
   awards_prefix, awards_suffix = "pitch_awards__", ""
   stats_prefix, stats_suffix = "pitcher_stats__", ""

   csv_filename = _create_filename_from_date(csv_dir, csv_prefix, day, csv_suffix)
   awards_filename = _create_filename_from_date(awards_dir, awards_prefix, day, awards_suffix)
   stats_filename = _create_filename_from_date(stats_dir, stats_prefix, day, stats_suffix)

   gid_urls = filter(has_game, GET_gid_urls(day))
   if not gid_urls: 
       print( "[ERR] no games were found for '{}'".format(d), file=sys.stdout)
       return
   gid_endpoints = map(lambda x: _map_to_file_if_exists(x, xml_dir ), gid_urls)
   gid_endpoints = map(lambda x: x.encode('ascii'), gid_endpoints)

   p = multiprocessing.Pool()

   GAMES = p.map(Game, gid_endpoints)
   GAMES = filter(lambda game: game.successful_xml_retrieval, GAMES)
   pitches = reduce(lambda x,y: x+y, [game.get_pitches() for game in GAMES])
   pitchers = build_pitcher_dict(
       reduce(lambda x,y: x+y, [g.get_pitchers() for g in GAMES])
   )

   pitches_to_csv(csv_filename, pitches)
   df = pd.read_csv(csv_filename) 

   for award in AWARD_ATTRS:
       write_award(df, award, pitchers, awards_filename) 

   write_pitcher_stats(df, pitchers, stats_filename)

   if p:
       con = _get_postgres_con("config.json")
       if con:
           _insert_pitches_into_postgres(csv_filename, "pitches{}".format(day.year),  con)
           con.close()
   if y:
       con = _get_postgres_con("config.json")
       players = reduce(lambda x,y: x+y, [g._players for g in GAMES])
       if con:
          _insert_players_into_postgres(players, "players", con)
          con.close()
   

if __name__ == "__main__":
   process_pitches()
