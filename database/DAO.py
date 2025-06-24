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



