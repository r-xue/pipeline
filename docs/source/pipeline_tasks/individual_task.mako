${name}
${"="*len(name)}

Task description
-----------------
${description}


Parameter List 
-------------
.. list-table::
    :widths: 25 50
    :header-rows: 1

    * - parameter name
      - description
% for parameter in parameters: 
    * - ${parameter.strip()}
      - | ${parameters[parameter].split("\n")[0].strip()}
    % for line in parameters[parameter].split("\n")[1:]:
        | ${line.strip()}
    % endfor
% endfor 

Examples 
--------
${examples}