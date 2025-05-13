from typing import List, NewType, Tuple, Union, Dict

ClusteringResult = NewType('ClusteringResult', Tuple[int, List[List[Union[int, bool]]], List[int], List[List[Union[int, float, bool]]]])
DetectedLineList = NewType('DetectedLineList', List[List[Union[int, float, bool]]])
LineProperty = NewType('LineProperty', List[Union[float, bool]])
LineWindow = NewType('LineWindow', Union[str, dict, List[int], List[float], List[str]])
FitFunc = NewType('FitFunc', Union[str, Dict[Union[int, str], str]])
FitOrder = NewType('FitOrder', Union[str, int, Dict[Union[int, str], int]])