build:
	pip install -r requirements.txt

testfolder:
	mkdir test/

clean:
	rm test/*

master:
	python master.py

backup:
	python b_master.py

worker:
	python slave.py