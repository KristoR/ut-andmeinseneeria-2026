-- LAHENDUS: Ulesanne 2B - Kohandatud test: orvuks jaanud postitused
--
-- Leiab postitused, mille kasutajat dim_users tabelis ei eksisteeri.
-- Test laebib, kui paering tagastab 0 rida.
-- Kui see paring tagastab ridu, tahendab see, et fct_posts
-- sisaldab viiteid kasutajatele, keda dim_users tabelis pole.

SELECT p.*
FROM {{ ref('fct_posts') }} p
LEFT JOIN {{ ref('dim_users') }} u
    ON p.user_key = u.user_key
WHERE u.user_key IS NULL
