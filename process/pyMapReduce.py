from multiprocessing import Process
from os import listdir, getpid
import numpy as np

class MapReduce(object):
    def __init__(self, map, reduce, m_task = 5, r_task = 3):
        self.map = map
        self.reduce = reduce
        self.m_task = m_task
        self.r_task = r_task

    @staticmethod
    def EmitIntermediate(ikey, val):
        f = open(f'./ikeys/{getpid()}', 'a')
        f.write(f'{ikey},{val}\n')
        f.close()

    def __call__(self, input):
        #split input data
        file = open(input, 'r')
        lines = np.array_split(np.array(file.readlines()), self.m_task)

        for i in range(self.m_task):
            nf = open(f'./inputs/input{i}', 'w')
            nf.writelines(lines[i].tolist())
            nf.close()

        ls = listdir('./inputs/')

        #call master process
        master = Process(target=self.__master__, args=([ls]))
        master.start()
        master.join()

        #recovery control
        print('Output in outputs folder')

    def __master__(self, files):
        prs = []
        dirs = []

        for f in files:
            p = Process(target=self.__map_worker__, args=(f, self.map))
            prs.append(p)
            p.start()
            dirs.append(p.pid)
        
        for p in prs:
            p.join()

        prs.clear()

        dirs = [fs.tolist() for fs in np.array_split(np.array(dirs), self.r_task)]
        
        for d in dirs:
            p = Process(target=self.__reduce_worker__, args=(d, self.reduce))
            prs.append(p)
            p.start()

        for p in prs:
            p.join() 
        

    def __map_worker__(self, file, map):

        def parse(line):
            k, v = tuple(line.split(','))
            v = v.split()
            return k, v
        
        f = open(f'./inputs/{file}', 'r')
        lines = f.readlines()
        for l in lines:    
            k,v = parse(l)
            map(k,v)
        
        f.close()

    def __reduce_worker__(self, files, reduce):

        def parse(line):
            k,v = tuple(line.split(','))
            return k, v

        ikey_dic = { }

        for f in files:
            d = open(f'./ikeys/{f}', 'r')
            for l in d.readlines():
                k, v = parse(l)
                try:
                    ikey_dic[k].append(v)
                except:
                    ikey_dic[k] = [v]

        out = open(f'./outputs/reduce{getpid()}', 'w')
        for k in ikey_dic.keys():
            out.write(f'{k} -> {reduce(k,ikey_dic[k])}\n')

        out.close() 