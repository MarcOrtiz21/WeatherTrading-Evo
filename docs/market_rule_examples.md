# Ejemplos de reglas de mercado reales (Mockup/Investigación)

Este documento recopila ejemplos de cómo Polymarket describe sus mercados meteorológicos para entrenar el parser.

## Ejemplo 1: Binario (Umbral)

**Título:** ¿Hará 33°C o más en Madrid el 15 de abril?
**Descripción:** Este mercado se resolverá a "Sí" si la temperatura máxima diaria registrada en el Aeropuerto de Madrid (LEMD) alcanza o supera los 33.0 grados Celsius el 15 de abril de 2026. En caso contrario, se resolverá a "No".
**Reglas de resolución:**
- Fuente: Wunderground (History Daily para LEMD).
- Variable: "Max Temperature".
- Redondeo: Se usará el primer decimal reportado. Si hay discrepancia entre fuentes, prevalecerá la fuente oficial citada.
- Fecha local: 15 de abril de 2026 (CEST).

## Ejemplo 2: Multinomial (Bins)

**Título:** ¿Cuál será la temperatura máxima en Londres el 20 de mayo?
**Resultados (Outcomes):**
- 15°C o menos
- 16°C
- 17°C
- 18°C
- 19°C o más
**Descripción:** Este mercado se resolverá según la temperatura máxima registrada en el Aeropuerto de Heathrow (EGLL) el 20 de mayo de 2026, según los datos históricos de la NOAA.
**Reglas de resolución:**
- Fuente: NOAA (Local Climatological Data).
- Variable: "Daily Maximum Temperature".
- Redondeo: Al entero más cercano. .5 redondea hacia arriba.

## Ejemplo 3: Ambiguo (Para validación)

**Título:** ¿Día caluroso en Barcelona?
**Descripción:** Si hace más de 30 grados en Barcelona el próximo lunes, el mercado es Sí.
**Problemas identificados:**
- No cita estación (¿Aeropuerto? ¿Centro ciudad?).
- No cita fuente (¿AEMET? ¿Wunderground?).
- No especifica si es 30.0 o 30.1.
- "Próximo lunes" es ambiguo si no hay fecha fija.
