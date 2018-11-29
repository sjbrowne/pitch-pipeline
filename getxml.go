package main

import (
    "flag"
    "fmt"
    "log"
    "time"

    "github.com/sjbrowne/mlbpipe"
)

func main() {
     // set flags
     yesterday := time.Now().AddDate(0,0,-1)
     default_date := fmt.Sprintf("%d-%02d-%02d", yesterday.Year(), yesterday.Month(), yesterday.Day())
     date_str_ptr := flag.String("d", default_date, "date in 'YYYY-MM-DD' format")
     opt_dir_ptr := flag.String("x", "", "optional directory to save xml")
     flag.Parse()

     t, err := time.Parse("2006-01-02", *date_str_ptr)
     if err != nil {
         log.Fatalf("date incorrectly formatted", err)
     }

     gameday := fmt.Sprintf(mlbpipe.MLBBaseURL + mlbpipe.DayFormat, t.Year(), t.Month(), t.Day())

     games := make(chan string)
     go mlbpipe.GetGames(gameday, games)
     mlbpipe.DelegateXMLWork(games, *opt_dir_ptr)
}

