{% macro calculate_discount_amount(discount, unit_price, quantity) %}
    (
        case 
            when {{ discount }} >= 1 
                then {{ unit_price }} * {{ quantity }} * ({{ discount }} / 100.0)
            else 
                {{ unit_price }} * {{ quantity }} * {{ discount }}
        end
    )
{% endmacro %}
