from constants import PITCH_TYPES, PITCH_STATS

def string_header(header):
    dash = "-"*len(header)
    title = "{}".format(header.upper())
    return dash + title + dash

def header(header, sep="-"):
    top =    "{}".format(sep)*len(header)
    title =  "{}".format(header.upper())
    bottom = "{}".format(sep)*len(header)
    return "\n".join([top, title, bottom]) + "\n"

def write_award(df, attr, pitchers, filename):
    f = open(filename, "a")

    f.write(string_header(attr))

    for pitch_type in PITCH_TYPES:
      dfpt = df[df.pitch_type == pitch_type]
      mx = dfpt.sort_values(attr, ascending=False).head(1)
      mn = dfpt.sort_values(attr, ascending=True).head(1)

      f.write("\n{}\n".format(pitch_type))

      if len(dfpt):
          min_pitcher = pitchers[str(mn.pitcher.values[0])]
          f.write( "MIN {} pitcher: {} \n".format(
            str(mn[attr].values[0]).rjust(5, " "), 
            min_pitcher
          ))

          max_pitcher = pitchers[str(mx.pitcher.values[0])]
          f.write( "MAX {} pitcher: {} \n".format(
            str(mx[attr].values[0]).rjust(5, " "), 
            max_pitcher
          ))
      else:
          f.write( "\n" )
    f.write("\n\n")
    f.close()

def write_pitcher_stats(df, pitchers, filename):
   """
   Params
   -----
      df : DataFrame
         all pitches
      P : dict 
         mapping of pitcher id [str] to pitcher name [str]
   """
   f = open(filename, "w")
      
   pitcher_group = df.groupby('pitcher')
   cols = ("min", "25%", "mean", "75%", "max", "std")
   stat_header = "{0:<11} {1:<11} {2:<11} {3:<11} {4:<11} {5:<11}\n".format(*cols)

   for id, df in pitcher_group:
      f.write(header("{}".format(pitchers[str(id)]), sep="="))

      for pitch_type in PITCH_TYPES: 
          f.write(header(pitch_type))
          for award in PITCH_STATS:
              pitch_type_frame = df[df["pitch_type"] == pitch_type]
              if len(pitch_type_frame) > 0:
                  f.write("_- "+award+" -_\n")
                  f.write(stat_header)
                  qrt1 = pitch_type_frame[award].quantile(q=0.25)
                  qrt3 = pitch_type_frame[award].quantile(q=0.75)
                  mean, std = pitch_type_frame[award].mean(), pitch_type_frame[award].std()
                  min, max = pitch_type_frame[award].min(), pitch_type_frame[award].max()
                  summary = "{0:<11.2f} {1:<11.2f} {2:<11.2f} {3:<11.2f} {4:<11.2f} {5:<11.2f} "
                  f.write(summary.format(min, qrt1, mean, qrt3, max, std))
                  f.write("\n\n")
   f.close()

