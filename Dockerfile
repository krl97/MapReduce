FROM python:3.7-slim
COPY . /project
RUN pip install ./project/requirements/dill-0.3.1.1.tar.gz
RUN pip install ./project/requirements/pyzmq-18.1.1.tar.gz
RUN pip install ./project/requirements/more-itertools-8.0.0.tar.gz
ENTRYPOINT [ "python", "/project/master.py" ]