from datetime import datetime

from pydantic.dataclasses import dataclass


@dataclass
class Pilot:
    driverId:int
    forename:str
    surname:str
    nationality:str

    def __hash__(self):
        return hash(self.driverId)
    def __eq__(self, other):
        return self.driverId==other.driverId
    def __str__(self):
        return f"{self.surname}"