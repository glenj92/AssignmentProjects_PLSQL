SET SERVEROUTPUT ON;

DECLARE
    onuError NUMBER;
    osbError VARCHAR2(100);
BEGIN
    LDC_pkGestionProyectos.prAsignarLenguajes(onuError, osbError);
    LDC_pkGestionProyectos.prAsignarProgrammerToProject(onuError, osbError);
    LDC_pkGestionProyectos.prGetPromedioProgramador(onuError, osbError);
    LDC_pkGestionProyectos.prUpdateProyectos(onuError, osbError);
    dbms_output.put_line(osbError);
EXCEPTION
    WHEN OTHERS THEN
        onuError := -1;
        osbError := 'Error no controlado en prAsignarLenguajes ' || sqlerrm;
        ROLLBACK;
        dbms_output.put_line(osbError);
END;
/