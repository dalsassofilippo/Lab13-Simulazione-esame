import copy

import networkx as nx

from database.DAO import DAO


class Model:
    def __init__(self):
        self._grafo=nx.DiGraph()
        self._drivers=[]
        self.idMap={}

        self._bestPath = []
        self._bestScore = 0

    def getDreamTeam(self, k):
        self._bestPath = []
        self._bestScore = 1000

        parziale = []
        self._ricorsione(parziale, k)
        return self._bestPath, self._bestScore

    def _ricorsione(self, parziale, k):
        if len(parziale) == k:
            if self.getScore(parziale) < self._bestScore:
                self._bestScore = self.getScore(parziale)
                self._bestPath = copy.deepcopy(parziale)
            return

        for n in self._grafo.nodes():
            if n not in parziale:
                parziale.append(n)
                self._ricorsione(parziale, k)
                parziale.pop()

    def getScore(self, team):
        score = 0
        for e in self._grafo.edges(data=True):
            if e[0] not in team and e[1] in team:
                score += e[2]["weight"]
        return score

    def creaGrafo(self,year):
        self._grafo.clear() #cancelliamo i grafi precedenti

        self._drivers=self.getNodes(year) #importiamo i nodi

        for nodo in self._drivers: #creiamo l'idMap
            self.idMap[nodo.driverId]=nodo

        self._grafo.add_nodes_from(self._drivers) #aggiungiamo i nodi

        allEdges=self.getEdges(year)
        for edge in allEdges: #aggiungiamo gli archi
            self._grafo.add_edge(edge[0],edge[1],weight=edge[2])

    def bestDriver(self):
        best=0
        bestDriver=None
        for n in self._grafo.nodes:
            score = 0
            for e_out in self._grafo.out_edges(n, data=True): #controllo arco in uscita e aggiungo
                score += e_out[2]["weight"]
            for e_in in self._grafo.in_edges(n, data=True): #controllo arco in entrata e sottraggo
                score -= e_in[2]["weight"]

            if score > best: #controllo swe lo score è migliore del best fino a quando non lo trovo
                bestDriver = n
                best = score

        return bestDriver,best

    def getEdges(self,year):
        return DAO.getEdges(year,self.idMap)
    def getYear(self):
        return DAO.getYear()
    def getNodes(self,year):
        return DAO.getNodes(year)
    def getGrafo(self):
        return list(self._grafo)
    def numNodi(self):
        return len(self._grafo.nodes)
    def numArchi(self):
        return len(self._grafo.edges)