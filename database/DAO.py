from database.DB_connect import DBConnect
from model.pilot import Pilot


class DAO():
    def __init__(self):
        pass

    @staticmethod
    def getYear():
        conn=DBConnect.get_connection()
        cursor=conn.cursor(dictionary=True)
        query="""select distinct(r.`year`)
                from races r 
                order by r.`year` desc"""
        cursor.execute(query)
        res=cursor.fetchall()
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes(year):
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select distinct d.driverId, d.forename, d.surname, d.nationality
                    from (select r.raceId,r2.circuitId,r.driverId
                    from results r, races r2
                    where r2.`year`=%s and r.raceId=r2.raceId and r.`position` is not null) p, drivers d
                    where p.driverid=d.driverId"""
        cursor.execute(query,(year,))
        res=[]
        for row in cursor.fetchall():
            res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges(year,idMap):
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query="""select r1.driverId as vincente, r2.driverId as sconfitto, count(*) as vittorie
				from results as r1, results as r2, races
				where r1.raceId = r2.raceId
				and races.raceId = r1.raceId
				and races.year = %s
				and r1.position is not null
				and r2.position is not null 
				and r1.position < r2.position 
				group by vincente, sconfitto"""
        cursor.execute(query,(year,))
        res=[]
        for row in cursor:
            res.append((idMap[row["vincente"]],idMap[row["sconfitto"]],row["vittorie"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes1(year): #tutti i piloti che hanno corso almeno una gara per un constructor
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """SELECT DISTINCT d.driverId, d.forename, d.surname, d.nationality
                    FROM results r,drivers d
                    where r.driverId = d.driverId;"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
            res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges1(year, idMap): #Quante volte due piloti hanno corso per lo stesso constructor, nella stessa gara.
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select r1.driverId as p1, r2.driverId as p2, count(*) as peso
                    from results r1, results r2
                    where r1.constructorId=r2.constructorId
                    and r1.driverId<r2.driverId
                    and r1.raceId=r2.raceId
                    group by p1,p2"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["p1"]], idMap[row["p2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes2(year): #piloti che hanno partecipato almeno a una qualifica
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select distinct d.driverId,d.forename,d.surname,d.nationality
                    from drivers d,qualifying q 
                    where d.driverId=q.driverId """
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
            res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges2(year, idMap): #un arco da A → B se A è partito più avanti in griglia rispetto a B nella stessa gara
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select r1.driverId as p1, r2.driverId as p2, count(*) as peso
                    from results r1, results r2
                    where r1.driverId <> r2.driverId
                    and r1.grid>r2.grid
                    and r1.raceId=r2.raceId
                    group by p1,p2"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["p1"]], idMap[row["p2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes3(year): #circuiti dove si è corso
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select distinct c.*
                    from circuits c, races r  
                    where c.circuitId=r.circuitId"""
        cursor.execute(query, (year,))
        res = []
        # for row in cursor.fetchall():
        #     res.append(Circuit(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges3(year, idMap): #due circuiti sono collegati se uno stesso constructor ha gareggiato in entrambi.
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """SELECT 
                        ra1.circuitId AS c1,
                        ra2.circuitId AS c2,
                        COUNT(DISTINCT r1.constructorId) AS peso
                    FROM results r1
                    JOIN races ra1 ON r1.raceId = ra1.raceId
                    JOIN results r2 ON r1.constructorId = r2.constructorId
                    JOIN races ra2 ON r2.raceId = ra2.raceId
                    WHERE ra1.circuitId < ra2.circuitId
                      AND ra1.circuitId <> ra2.circuitId
                      AND ra1.raceId <> ra2.raceId
                    GROUP BY ra1.circuitId, ra2.circuitId;"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["c1"]], idMap[row["c2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes4(year): # piloti che hanno terminato almeno una gara nell'anno %s.
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select distinct d.driverId, d.forename, d.surname, d.nationality
                    from drivers d, results r, races r1
                    where d.driverId=r.driverId 
                    and r.`position` is not null
                    and r1.raceId=r.raceId
                    and r1.`year`=%s"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
             res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges4(year, idMap): #A → B se A ha superato B in gara (partiva dietro e finiva davanti nella stessa gara) nell'anno %s
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select r1.driverId as d1, r2.driverId as d2, count(*) as peso
                    from results r1, results r2, races r
                    where r1.raceId=r2.raceId
                    and r1.raceId=r.raceId
                    and  r.`year`=%s
                    and r1.driverId<>r2.driverId
                    and r1.grid<r2.grid
                    and r1.`position`>r2.`position`
                    and r1.position IS NOT NULL
                    and r2.position IS NOT NULL
                    group by d1,d2"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["d1"]], idMap[row["d2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes5(year): # nazionalità coinvolte in almeno una gara con risultato valido nell'anno %s
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select distinct d.nationality
                    from drivers d, races r, results r2 
                    where r.raceId=r2.raceId
                    and r.`year`=2015
                    and r2.driverId=d.driverId
                    and r2.`position` is not null"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
             res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges5(year, idMap): #arco da A → B se i piloti di nazione A hanno battuto (cioè sono arrivati davanti a) piloti di nazione B in almeno una gara nell'anno %s
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select d1.nationality as n1, d2.nationality as n2, count(*) as peso
                    from drivers d1, drivers d2, results r1,results r2, races r
                    where r1.raceId=r.raceId
                    and r2.raceId=r.raceId 
                    and r.`year`=2015
                    and r1.driverId<>r2.driverId
                    and r1.driverId=d1.driverId
                    and r2.driverId=d2.driverId
                    and r1.`position`>r2.`position`
                    and r1.`position` is not null
                    and r2.`position` is not null
                    group by n1,n2"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["n1"]], idMap[row["n2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes6(year): # costruttori nell'anno %s
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select distinct c.*
                    from results r, constructors c, races r2 
                    where r.raceId=r2.raceId
                    and r2.`year`=%s
                    and r.constructorId=c.constructorId"""
        cursor.execute(query, (year,))
        res = []
        # for row in cursor.fetchall():
        #     res.append(Circuit(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges6(year, idMap): #  C1 → C2 se C1 ha finito più gare con almeno 2 piloti al traguardo rispetto a C2 nell'anno %s
                                #(usa subquery per contare le gare terminate con ≥2 piloti per ogni constructor)
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """SELECT 
                        c1.constructorId AS più_affidabile,
                        c2.constructorId AS meno_affidabile,
                        c1.num_gare - c2.num_gare AS diff
                    from (SELECT constructorId, COUNT(*) AS num_gare
                      from (
                      SELECT 
                          r.constructorId,
                          r.raceId,
                          COUNT(*) AS count_finishers
                      FROM results r, races r1
                      WHERE r.position IS NOT null 
                      and r.raceId=r1.raceId
                      and r1.year=2015
                      GROUP BY r.constructorId, r.raceId
                      HAVING count_finishers >= 2) gare_per_costruttore
                      GROUP BY constructorId
                    ) c1, (SELECT constructorId, COUNT(*) AS num_gare
                      from (
                      SELECT 
                          r.constructorId,
                          r.raceId,
                          COUNT(*) AS count_finishers
                      FROM results r, races r1
                      WHERE r.position IS NOT null 
                      and r.raceId=r1.raceId
                      and r1.year=2015
                      GROUP BY r.constructorId, r.raceId
                      HAVING count_finishers >= 2) gare_per_costruttore
                      GROUP BY constructorId
                    ) c2
                    where c1.constructorId <> c2.constructorId
                    and c1.num_gare > c2.num_gare;"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["più_affidabile"]], idMap[row["meno_affidabile"]], row["diff"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes7(year):  # input anno di gara-> piloti che hanno concluso almeno una gara nell'anno %s
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """SELECT DISTINCT d.driverId, d.forename, d.surname, d.nationality
                    FROM results r, drivers d,races ra
                    WHERE r.driverId = d.driverId
                    and r.raceId = ra.raceId
                    and r.position IS NOT NULL
                    and ra.year =%s"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
             res.append(Pilot(dizionario={},**row)) #PER ESAME
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges7(year,idMap):  #(pilota A ha vinto su B nella stessa gara)
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select r1.driverId as d1, r2.driverId as d2, count(*) as peso
                    from results r1, results r2, races r 
                    where r1.raceId=r2.raceId and r1.raceId=r.raceId and r.`year`=2015
                    and r1.driverId<>r2.driverId
                    and r1.`position` is not null
                    and r2.`position` is not null
                    and r1.`position` < r2.`position`
                    group by d1,d2"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["d1"]], idMap[row["d2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes8(year):  #piloti della nazione selezionata con almeno una gara arrivata
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """SELECT DISTINCT d.driverId, d.forename, d.surname, d.nationality
                    from drivers d, results r
                    where d.nationality='German' and d.driverId=r.driverId and r.`position` is not null"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
            res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges8(year, idMap):  #sorpasso da A a B (partiva dietro, finiva davanti)
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select d1.driverId as d1, d2.driverId as d2, count(*) as peso
                    from results r1, results r2, (SELECT DISTINCT d.driverId, d.forename, d.surname, d.nationality
                    from drivers d, results r
                    where d.nationality='German' and d.driverId=r.driverId and r.`position` is not null) d1,
                    (SELECT DISTINCT d.driverId, d.forename, d.surname, d.nationality
                    from drivers d, results r
                    where d.nationality='German' and d.driverId=r.driverId and r.`position` is not null) d2
                    where d1.driverId<>d2.driverId
                    and d1.driverId=r1.driverId
                    and d2.driverid=r2.driverId
                    and r1.raceId=r2.raceId
                    and r1.`position` is not null
                    and r2.`position` is not null
                    and r1.grid > r2.grid
                    and r1.`position` < r2.`position`
                    group by d1,d2"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["d1"]], idMap[row["d2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getNodes9(year):  #costruttori che hanno ottenuto almeno @min_podi podi (posizioni 1–3)
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """select p.*
                    from (select c.*, count(*) as num_podi
                    from constructors c, results r 
                    where c.constructorId=r.constructorId and r.`position`<=3
                    group by c.constructorId) p
                    where p.num_podi>=%s"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor.fetchall():
            res.append(Pilot(**row))
        cursor.close()
        conn.close()
        return res

    @staticmethod
    def getEdges9(year, idMap):  #C1 → C2 se C1 ha finito più gare con almeno 2 piloti al traguardo rispetto a C2
        conn = DBConnect.get_connection()
        cursor = conn.cursor(dictionary=True)
        query = """with podi_per_costruttore_gara AS (
  SELECT constructorId, raceId, COUNT(*) AS num_podi
  FROM results
  WHERE positionOrder <= 3
  GROUP BY constructorId, raceId
), nodi as (select p.*
from (select c.*, count(*) as num_podi
from constructors c, results r 
where c.constructorId=r.constructorId and r.`position`<=3
group by c.constructorId) p
where p.num_podi>=99)
SELECT 
  a.constructorId AS vincente,
  b.constructorId AS sconfitto,
  COUNT(*) AS gare_superate
FROM podi_per_costruttore_gara a
JOIN podi_per_costruttore_gara b
  ON a.raceId = b.raceId
JOIN nodi na ON na.constructorId = a.constructorId
JOIN nodi nb ON nb.constructorId = b.constructorId
WHERE a.num_podi > b.num_podi
GROUP BY a.constructorId, b.constructorId;"""
        cursor.execute(query, (year,))
        res = []
        for row in cursor:
            res.append((idMap[row["d1"]], idMap[row["d2"]], row["peso"]))
        cursor.close()
        conn.close()
        return res

nodi="""SELECT DISTINCT c.*
FROM circuits c
JOIN races r ON c.circuitId = r.circuitId
WHERE r.year BETWEEN @annoMin AND @annoMax;"""

diz="""SELECT r.year, res.driverId, res.position
FROM races r
JOIN results res ON r.raceId = res.raceId
WHERE r.circuitId = @circuitId
  AND r.year BETWEEN @annoMin AND @annoMax
  AND res.position IS NOT NULL
ORDER BY r.year;"""

archi="""select r.circuitId, r2.circuitld
from races r, races r2, results r3, results r4
where r.circuitId<r2.circuitId and r4.raceld=r.raceld 
and r3.raceld=r2.raceld 
and r2.`year`<=2016 and r2.`year`>=2010 
and r.`year`<=2016 and r.`year`>=2010 
group by r.circuitId, r2.circuitId"""

@dataclass
class Piazzamento:
    driverId: int
    position: int

@dataclass
class Circuito:
    circuitId: int
    name: str
    location: str
    country: str
    ...
    risultati_per_anno: dict[int, list[Piazzamento]]


"""🔗 C) Query per gli archi tra circuiti
Step 1: trova le coppie di circuiti attivi nello stesso anno
sql

WITH circuiti_attivi AS (
  SELECT DISTINCT circuitId, year
  FROM races
  WHERE year BETWEEN @annoMin AND @annoMax
)
SELECT ca1.circuitId AS c1, ca2.circuitId AS c2, ca1.year
FROM circuiti_attivi ca1
JOIN circuiti_attivi ca2 
  ON ca1.year = ca2.year AND ca1.circuitId < ca2.circuitId
Step 2: calcola il peso dell’arco (piloti classificati nei 2 circuiti, per ogni anno in comune)
sql

WITH gare_finite AS (
  SELECT r.circuitId, r.year, COUNT(DISTINCT res.driverId) AS piloti_classificati
  FROM races r
  JOIN results res ON r.raceId = res.raceId
  WHERE r.year BETWEEN @annoMin AND @annoMax
    AND res.position IS NOT NULL
  GROUP BY r.circuitId, r.year
),
coppie_anni_comuni AS (
  SELECT g1.circuitId AS c1, g2.circuitId AS c2, g1.year
  FROM gare_finite g1
  JOIN gare_finite g2
    ON g1.year = g2.year AND g1.circuitId < g2.circuitId
)
SELECT 
  c1, c2,
  SUM(g1.piloti_classificati + g2.piloti_classificati) AS peso
FROM coppie_anni_comuni c
JOIN gare_finite g1 ON c.c1 = g1.circuitId AND c.year = g1.year
JOIN gare_finite g2 ON c.c2 = g2.circuitId AND c.year = g2.year
GROUP BY c1, c2;

🧠 In Python: struttura arco
python
Copia codice
@dataclass
class ArcoCircuiti:
    circuito1: int
    circuito2: int
    peso: int"""


