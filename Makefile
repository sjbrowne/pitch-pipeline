
getxml: getxml.go
	go build getxml.go

clean_data:
	rm -rf games_*
	rm -rf pitcher_* 
	rm -rf pitch_*

clean:
	rm getxml
