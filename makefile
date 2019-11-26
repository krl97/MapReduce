testfolder:
	mkdir test/

clean:
	rm test/*

run:
	echo -----------------------------------------------
	echo Sending Clients to Background
	python client.py "8082" 1 &
	python client.py "8084" 2 &
	python client.py "8086" 3 &
	python client.py "8088" 4 &

	echo Running Server in Foreground
	python server.py