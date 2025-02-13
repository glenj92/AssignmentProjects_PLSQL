CREATE OR REPLACE PACKAGE LDC_pkGestionProyectos IS
    TYPE refCursor IS REF CURSOR;
    TYPE rcdProgramador IS RECORD (
        cNombProgramador VARCHAR2(500),
        cAnioExperience  ldc_programador.PGANEXP%TYPE,
        cPuntoExperLegun LDC_PROGLEGPRO.PLPPUNEX%TYPE,
        cNombProyecto    ldc_proyecto.PYNOMBR%TYPE,
        cValorProyecto   ldc_proyecto.PYVALOR%TYPE,
        cRowidPoyecto    ROWID
    );
    TYPE tblRcdProgramador IS
        TABLE OF rcdProgramador INDEX BY PLS_INTEGER;
    PROCEDURE prAsignarLenguajes (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    );

    PROCEDURE prAsignarProgrammerToProject (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    );

    FUNCTION fnGetPromedioProgramador (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    ) RETURN refCursor;

    PROCEDURE prGetPromedioProgramador (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    );

    PROCEDURE prUpdateProyectos (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    );

END LDC_pkGestionProyectos;
/

CREATE OR REPLACE PACKAGE BODY LDC_pkGestionProyectos IS

    PROCEDURE prAsignarLenguajes (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    ) IS

        v_id_programador NUMBER;
        v_id_lenguaje    NUMBER;
        CURSOR cuProgramadores IS
        SELECT
            PGIDENT
        FROM
            ldc_programador;

        CURSOR cuLenguajes (
            p_id_programador NUMBER
        ) IS
        SELECT
            LPIDENT
        FROM
            (
                SELECT
                    LPIDENT,
                    LPNOMBR
                FROM
                    ldc_lenguprog
                WHERE
                        LPESTAD = 'A'
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            LDC_PROGLEGPRO
                        WHERE
                                PLPPROGR = v_id_programador
                            AND PLPLENGU = ldc_lenguprog.LPIDENT
                    )
                ORDER BY
                    DBMS_RANDOM.VALUE
            )
        WHERE
            ROWNUM <= 3;

    BEGIN
        OPEN cuProgramadores;
        LOOP
            FETCH cuProgramadores INTO v_id_programador;
            EXIT WHEN cuProgramadores%NOTFOUND;
            OPEN cuLenguajes(v_id_programador);
            LOOP
                FETCH cuLenguajes INTO v_id_lenguaje;
                EXIT WHEN cuLenguajes%NOTFOUND;
                MERGE INTO LDC_PROGLEGPRO plp
                USING (
                    SELECT
                        v_id_programador id_programador,
                        v_id_lenguaje    id_lenguaje
                    FROM
                        dual
                ) src ON ( plp.PLPPROGR = src.id_programador
                           AND plp.PLPLENGU = src.id_lenguaje )
                WHEN NOT MATCHED THEN
                INSERT (
                    PLPPROGR,
                    PLPLENGU,
                    PLPPUNEX )
                VALUES
                    ( src.id_programador,
                      src.id_lenguaje,
                    ROUND(DBMS_RANDOM.VALUE(0, 100),
                          0) );

                COMMIT;
            END LOOP;

            CLOSE cuLenguajes;
        END LOOP;

        CLOSE cuProgramadores;
    EXCEPTION
        WHEN OTHERS THEN
            onuError := -1;
            osbError := 'Error no controlado en prAsignarLenguajes ' || sqlerrm;
            IF cuProgramadores%isOpen THEN
                CLOSE cuProgramadores;
            END IF;
            IF cuLenguajes%isOpen THEN
                CLOSE cuLenguajes;
            END IF;
            ROLLBACK;
            dbms_output.put_line(osbError);
    END prAsignarLenguajes;

    PROCEDURE prAsignarProgrammerToProject (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    ) IS

        v_id_programador NUMBER;
        countProyProg    NUMBER := 0;
        contador         NUMBER := 0;
--cursor Obtinene los proyectos en estado y Fechas Vigentes
        CURSOR cuProyectos IS
        SELECT
            PYIDENT ident,
            PYLEPRO lenguaje,
            PYCAPRO cantProgRequerido
        FROM
            LDC_PROYECTO
        WHERE
                LDC_PROYECTO.PYESTAD = 'V'
            AND sysdate BETWEEN pyfeini AND COALESCE(pyfefin, sysdate);

        reg_proyecto     cuProyectos%ROWTYPE;
    
--cursor obtiene los programadores con el lenguae de programacion requerido y que no esten asignados en el mismo proyecto
        CURSOR cuProgramadoresXlenguaje (
            inuProyecto NUMBER,
            inuLenguaje NUMBER
        ) IS
        SELECT
            p.PLPPROGR programador
        FROM
            LDC_PROGLEGPRO p
        WHERE
                p.PLPLENGU = inuLenguaje
            AND NOT EXISTS (
                SELECT
                    1
                FROM
                    LDC_PROGPROYECTO
                WHERE
                        PPPROYE = inuProyecto
                    AND PPPROGR = p.PLPPROGR
            )
        ORDER BY
            p.PLPPUNEX DESC;
        
       
        
    --cursor valida si la cantidad de proyectos activos a la que un programador pertenece no se mayor que 2 y que que la cantidad permitida de programadores por proyecto no supere la requerida
        CURSOR cuProyProg (
            inuProgramador NUMBER,
            inuProyecto    NUMBER
        ) IS
        SELECT
            COUNT(1)
        FROM
            LDC_PROYECTO
        WHERE
                PYIDENT = inuProyecto
            AND PYCAPRO > (
                SELECT
                    COUNT(1)
                FROM
                    LDC_PROGPROYECTO
                WHERE
                    PPPROYE = inuProyecto
            )
            AND 2 > (
                SELECT
                    COUNT(1)
                FROM
                         LDC_PROYECTO py
                    JOIN LDC_PROGPROYECTO ppy ON py.PYIDENT = ppy.PPPROYE
                WHERE
                        ppy.PPPROGR = inuProgramador
                    AND py.PYESTAD = 'V'
                    AND sysdate BETWEEN py.pyfeini AND COALESCE(py.pyfefin, sysdate)
            );

    BEGIN
        OPEN cuProyectos;
        LOOP
            FETCH cuProyectos INTO reg_proyecto;
            EXIT WHEN cuProyectos%NOTFOUND;
            contador := 0;
            OPEN cuProgramadoresXlenguaje(reg_proyecto.ident, reg_proyecto.lenguaje);
            LOOP
                FETCH cuProgramadoresXlenguaje INTO v_id_programador;
                EXIT WHEN cuProgramadoresXlenguaje%NOTFOUND;
                OPEN cuProyProg(v_id_programador, reg_proyecto.ident);
                FETCH cuProyProg INTO countProyProg;
                CLOSE cuProyProg;
                IF countProyProg = 1 THEN
                    INSERT INTO LDC_PROGPROYECTO (
                        PPPROGR,
                        PPPROYE
                    ) VALUES (
                        v_id_programador,
                        reg_proyecto.ident
                    );

                    COMMIT;
                    contador := contador + 1;
                END IF;

            END LOOP;

            CLOSE cuProgramadoresXlenguaje;
        END LOOP;

        CLOSE cuProyectos;
    EXCEPTION
        WHEN OTHERS THEN
            onuError := -1;
            osbError := 'Error no controlado en prAsignarProgrammerToProject ' || sqlerrm;
            IF cuProyectos%isOpen THEN
                CLOSE cuProyectos;
            END IF;
            IF cuProgramadoresXlenguaje%isOpen THEN
                CLOSE cuProgramadoresXlenguaje;
            END IF;
           -- ROLLBACK;
            dbms_output.put_line(osbError);
    END prAsignarProgrammerToProject;

    FUNCTION fnGetPromedioProgramador (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    ) RETURN refCursor IS
        v_refCursor refCursor;
    BEGIN
        onuError := 0;
        OPEN v_refCursor FOR SELECT
                                                      PGPRNOM
                                                      || ' '
                                                      || PGSENOM
                                                      || ' '
                                                      || PGPRAPE
                                                      || ' '
                                                      || PGSEAPE nombre,
                                                      round(AVG(plp.PLPPUNEX),
                                                            1)   promedio
                                                  FROM
                                                           LDC_PROGRAMADOR p
                                                      JOIN LDC_PROGLEGPRO plp ON p.PGIDENT = plp.PLPPROGR
                             GROUP BY
                                 PGPRNOM
                                 || ' '
                                 || PGSENOM
                                 || ' '
                                 || PGPRAPE
                                 || ' '
                                 || PGSEAPE
                             ORDER BY
                                 promedio DESC;

        RETURN v_refCursor;
    EXCEPTION
        WHEN OTHERS THEN
            onuError := -1;
            osbError := 'Error no controlado en fnGetPromedioProgramador ' || sqlerrm;
            RETURN v_refCursor;
    END fnGetPromedioProgramador;

    PROCEDURE prGetPromedioProgramador (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    ) IS
        v_refCursor LDC_pkGestionProyectos.refCursor;
        sbNombre    VARCHAR2(500);
        v_promedio  NUMBER;
    BEGIN
        v_refCursor := LDC_pkGestionProyectos.fnGetPromedioProgramador(onuError, osbError);
        IF onuError = 0 THEN
            LOOP
                FETCH v_refCursor INTO
                    sbNombre,
                    v_promedio;
                EXIT WHEN v_refCursor%notfound;
                dbms_output.put_line('Nombre del Programador -> '
                                     || sbNombre
                                     || ' '
                                     || 'Promedio -> '
                                     || v_promedio);

            END LOOP;

            CLOSE v_refCursor;
        ELSE
            dbms_output.put_line(onuError
                                 || ' '
                                 || 'Ocurrio un error');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            onuError := -1;
            osbError := 'Error no controlado en prGetPromedioProgramador ' || sqlerrm;
    END prGetPromedioProgramador;

    PROCEDURE prUpdateProyectos (
        onuError OUT NUMBER,
        osbError OUT VARCHAR2
    ) IS

        CURSOR cuRegProyecto IS
        SELECT
            p.PGPRNOM
            || ' '
            || p.PGPRAPE nombreProgra,
            p.PGANEXP    anoExperiencia,
            (
                SELECT
                    plp.PLPPUNEX
                FROM
                    LDC_PROGLEGPRO plp
                WHERE
                        plp.PLPPROGR = t1.programadoId
                    AND plp.PLPLENGU = t1.lenguajeProyecto
            )            puntoExperiencia,
            t1.nombreProyecto,
            t1.valorProyecto,
            t1.IDrow
        FROM
                 (
                SELECT
                    (
                        SELECT
                            PPPROGR
                        FROM
                            LDC_PROGPROYECTO
                        WHERE
                                PPPROYE = py.PYIDENT
                            AND ROWNUM <= 1
                    )          programadoId,
                    py.PYIDENT idProyecto,
                    py.PYLEPRO lenguajeProyecto,
                    py.PYNOMBR nombreProyecto,
                    py.PYVALOR valorProyecto,
                    py.rowid   IDrow
                FROM
                    LDC_PROYECTO py
                WHERE
                    py.PYESTAD <> 'T'
                ORDER BY
                    py.PYVALOR
            ) t1
            JOIN LDC_PROGRAMADOR p ON p.PGIDENT = t1.programadoId
        WHERE
            t1.programadoId IS NOT NULL
            AND ROWNUM <= 5;

        v_tblRcdProgramador tblRcdProgramador;
    BEGIN
        OPEN cuRegProyecto;
        LOOP
            FETCH cuRegProyecto
            BULK COLLECT INTO v_tblRcdProgramador;
            FORALL i IN 1..v_tblRcdProgramador.COUNT
                UPDATE LDC_PROYECTO
                SET
                    PYESTAD = 'T'
                WHERE
                    ROWID = v_tblRcdProgramador(i).cRowidPoyecto;

            COMMIT;
            EXIT WHEN cuRegProyecto%NOTFOUND;
        END LOOP;

        CLOSE cuRegProyecto;
    EXCEPTION
        WHEN OTHERS THEN
            onuError := -1;
            osbError := 'Error no controlado en prUpdateProyectos ' || sqlerrm;
    END prUpdateProyectos;

END LDC_pkGestionProyectos;
/