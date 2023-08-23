${name}
${"="*len(name)}

% if description: 
Task description
-----------------
${description}
% endif

% if parameters: 
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
      % if line:
        | ${line.strip()}
      % endif
    % endfor
% endfor 
% endif


% if examples:
Examples 
--------
${examples}
% endif