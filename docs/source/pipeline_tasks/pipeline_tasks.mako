List of Heuristics Tasks (Pipeline ${plversion})
=================================================

% for category in pdict:
    
${task_groups[category]}
${"-"*len(task_groups[category])}
${len(pdict[category])} tasks available. 

.. list-table::
    :widths: 25 50
    :header-rows: 1

    * - task name
      - description
% for task in pdict[category]: 
    * - ${task[0]}
      - ${task[1]}
% endfor 

.. toctree::
   :maxdepth: 1
   :glob:

   ${category}/*

% endfor 


