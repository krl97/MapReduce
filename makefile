testfolder:
	mkdir test/

clean:
	rm test/*

run: testfolder
	echo Running Server in Foreground
	python master.py

	echo -----------------------------------------------
	echo Sending Clients to Background
	python slave.py "8082" 1 &
	python slave.py "8084" 2 &
	python slave.py "8086" 3 &
	python slave.py "8088" 4 &