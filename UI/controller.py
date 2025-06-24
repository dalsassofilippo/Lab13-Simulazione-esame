import flet as ft
from networkx.algorithms.bipartite.basic import color


class Controller:
    def __init__(self, view, model):
        # the view, with the graphical elements of the UI
        self._view = view
        # the model, which implements the logic of the program and holds the data
        self._model = model

    def fillDDYear(self):
        anni=self._model.getYear()
        for a in anni:
            self._view._ddAnno.options.append(ft.dropdown.Option(a["year"]))
        self._view.update_page()

    def handleDDYearSelection(self, e):
        pass

    def handleCreaGrafo(self,e):
        anno = self._view._ddAnno.value
        self._view.txt_result.clean()
        if anno is None:
            self._view.txt_result.clean()
            self._view.txt_result.controls.append(ft.Text("Attenzione: selezionare l'anno!",color="red"))
            self._view.update_page()
            return
        self._model.creaGrafo(anno)
        grafo=self._model.getGrafo()
        if grafo is None:
            self._view.txt_result.clean()
            self._view.txt_result.controls.append(ft.Text("Grafo non presente!",color="red"))
            self._view.update_page()
            return
        self._view.txt_result.clean()
        self._view.txt_result.controls.append(ft.Text("Grafo correttamente creato:"))
        self._view.txt_result.controls.append(ft.Text(f"Numero di nodi: {self._model.numNodi()}"))
        self._view.txt_result.controls.append(ft.Text(f"Numero di archi: {self._model.numArchi()}"))
        self._view.txt_result.controls.append(ft.Text(f"Best driver: {self._model.bestDriver()[0]}, with score {self._model.bestDriver()[1]}"))
        self._view.update_page()

    def handleCerca(self, e):
        k = self._view._txtIntK.value
        kint = int(k)

        path, scoretot = self._model.getDreamTeam(kint)
        self._view.txt_result.controls.clear()
        self._view.txt_result.controls.append(
            ft.Text(f"Il Dream Team con il minor tasso di sconfitta pari a {scoretot} è:"))
        for p in path:
            self._view.txt_result.controls.append(ft.Text(p))
        self._view.update_page()