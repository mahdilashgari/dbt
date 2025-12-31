{% macro url_encode(column_name) %}
    {% set replacements = {
        '\\\%': '%25',
        '\\\!': '%21',
        '\\\/': '%2F',
        '\\\#': '%23',
        '\\\$': '%24',
        '\\\&': '%26',
        '\\\'': '%27',
        '\\\(': '%28',
        '\\\)': '%29',
        '\\\*': '%2A',
        '\\\+': '%2B',
        '\\\,': '%2C',
        '\\\:': '%3A',
        '\\\;': '%3B',
        '\\\=': '%3D',
        '\\\?': '%3F',
        '\\\@': '%40',
        '\\\[': '%5B',
        '\\\]': '%5D',
        '\\\`': '%60',
        '\\\~': '%7E'
    } %}
    
    {% set holder = [column_name] %}

    {% for char, code in replacements.items() %}
        {% set encoded_string = 
            "regexp_replace(" 
            ~ holder[-1] 
            ~ ", '" 
            ~ char
            ~ "', '" 
            ~ code 
            ~ "')" %}
        {% do holder.append(encoded_string) %}
    {% endfor %}
    
    {{ holder[-1] }}
{% endmacro %}

