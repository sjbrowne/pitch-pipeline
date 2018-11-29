import datetime
import sys

if __name__ == "__main__":
   if len(sys.argv) != 2:
       sys.exit(1)
   #print datetime.datetime(sys.argv[1]).year
   print datetime.datetime.strptime(sys.argv[1],"%Y-%m-%d").year
