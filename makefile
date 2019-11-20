testfolder:
	mkdir test test/map_results

clean:
	rm test/map_results/* test/output

run:
	echo Sending Clients to Background
	python client.py "8082" &
	python client.py "8083" &
	python client.py "8084" &
	python client.py "8085" &

	echo Running Server in Foreground
	python server.py