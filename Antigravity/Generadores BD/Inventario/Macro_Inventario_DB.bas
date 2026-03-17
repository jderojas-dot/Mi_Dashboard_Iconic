'==============================================================================
' MACRO: Crear_Hoja_Inventario
' DESCRIPCION: Crea una nueva hoja ("Inventario_DB") copiando solo las columnas
'              seleccionadas desde la hoja "Sheet" del archivo
'              "Movimientos Inventario.xls / .xlsx"
' INSTRUCCIONES:
'   1. Abrir el archivo "Movimientos Inventario" en Excel
'   2. Presionar Alt + F11 para abrir el Editor de VBA
'   3. Ir a Insertar > Modulo
'   4. Pegar todo este codigo en el modulo
'   5. Cerrar el editor y volver a Excel
'   6. Presionar Alt + F8, seleccionar "Crear_Hoja_Inventario" y ejecutar
'   (Opcional) Asignar la macro a un boton en la hoja para ejecucion rapida
'==============================================================================

Sub Crear_Hoja_Inventario()

    Dim wb As Workbook
    Set wb = ThisWorkbook

    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual

    ' Las columnas se dividen en 3 grupos para evitar el error
    ' "demasiadas continuaciones de linea" de VBA (limite: 25 por bloque)
    Dim parte1 As Variant
    Dim parte2 As Variant
    Dim parte3 As Variant
    Dim colsInventario As Variant
    Dim i As Long
    Dim idx As Long
    Dim totalCols As Long

    parte1 = Array("C" & Chr(243) & "digo Inventario", "Fecha Movimiento", "Ejercicio", _
                   "Registro Contable", "Tipo Operaci" & Chr(243) & "n", "Tipo Movimiento", _
                   "C" & Chr(243) & "digo", "Descripci" & Chr(243) & "n", _
                   "Alm" & Chr(97) & Chr(99) & Chr(233) & "n", "Indicador Entrada - Salida")

    parte2 = Array("Cantidad", "Costo Moneda Nacional", "Costo Total Moneda Nacional", _
                   "Costo Moneda Extranjera", "Costo Total Moneda Extranjera", "Moneda", _
                   "Tipo de Cambio", "Usuario Creaci" & Chr(243) & "n", "Fecha Registro")

    parte3 = Array("Usuario Modificaci" & Chr(243) & "n", "Fecha Modificaci" & Chr(243) & "n", _
                   "Tip. Doc. Auxiliar", "Nro. Doc. Auxiliar", "Nombre Auxiliar", _
                   "Tip. Doc.", "Serie", "NroDoc")

    ' Combinar los tres grupos en un solo array
    totalCols = (UBound(parte1) + 1) + (UBound(parte2) + 1) + (UBound(parte3) + 1)
    ReDim colsInventario(0 To totalCols - 1)

    idx = 0
    For i = 0 To UBound(parte1): colsInventario(idx) = parte1(i): idx = idx + 1: Next i
    For i = 0 To UBound(parte2): colsInventario(idx) = parte2(i): idx = idx + 1: Next i
    For i = 0 To UBound(parte3): colsInventario(idx) = parte3(i): idx = idx + 1: Next i

    ' Fila 4 contiene encabezados, datos desde fila 5
    Call CrearHojaFiltrada(wb, "Sheet", "Inventario_DB", colsInventario, 4)

    ' --- Agregar columna "Clave_Inventario" (Serie & "-" & NroDoc & Codigo & Cantidad) ---
    Call AgregarClaveInventario(wb, "Inventario_DB")

    Application.ScreenUpdating = True
    Application.Calculation = xlCalculationAutomatic

    MsgBox "Proceso completado exitosamente." & vbCrLf & vbCrLf & _
           "Se creo/actualizo la hoja: Inventario_DB (con columna Clave_Inventario)", _
           vbInformation, "Hoja Filtrada"

End Sub



'==============================================================================
' SUBRUTINA: AgregarClaveInventario
' Agrega al final de Inventario_DB la columna "Clave_Inventario"
' formada por: Serie & "-" & NroDoc & Codigo & Cantidad (sin decimales)
'==============================================================================

Sub AgregarClaveInventario(wb As Workbook, sHoja As String)

    Dim ws As Worksheet
    Dim ultimaCol As Long, ultimaFila As Long
    Dim colSerie As Long, colNroDoc As Long
    Dim colCodigo As Long, colCantidad As Long
    Dim i As Long, j As Long

    Set ws = wb.Sheets(sHoja)

    ultimaFila = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    ultimaCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    ' Buscar las columnas necesarias en la fila de encabezados (fila 1)
    colSerie = 0
    colNroDoc = 0
    colCodigo = 0
    colCantidad = 0
    For j = 1 To ultimaCol
        Select Case Trim(ws.Cells(1, j).Value)
            Case "Serie":    colSerie = j
            Case "NroDoc":   colNroDoc = j
            Case "C" & Chr(243) & "digo": colCodigo = j
            Case "Cantidad": colCantidad = j
        End Select
    Next j

    If colSerie = 0 Or colNroDoc = 0 Or colCodigo = 0 Or colCantidad = 0 Then
        MsgBox "No se encontraron todas las columnas necesarias para Clave_Inventario." & _
               vbCrLf & "(Serie, NroDoc, C" & Chr(243) & "digo, Cantidad)", vbExclamation
        Exit Sub
    End If

    ' Agregar encabezado "Clave_Inventario" en la siguiente columna disponible
    Dim colClave As Long
    colClave = ultimaCol + 1

    With ws.Cells(1, colClave)
        .Value = "Clave_Inventario"
        .Font.Bold = True
        .Font.Color = RGB(255, 255, 255)
        .Interior.Color = RGB(31, 73, 125)
    End With

    ' Rellenar Clave_Inventario para cada fila de datos
    ' Formato: Serie & "-" & NroDoc & Codigo & Cantidad (sin decimales)
    For i = 2 To ultimaFila
        If ws.Cells(i, 1).Value <> "" Then
            ws.Cells(i, colClave).Value = CStr(ws.Cells(i, colSerie).Value) & "-" & _
                                          CStr(ws.Cells(i, colNroDoc).Value) & _
                                          CStr(ws.Cells(i, colCodigo).Value) & _
                                          CStr(CLng(ws.Cells(i, colCantidad).Value))
        End If
    Next i

    ws.Columns(colClave).AutoFit

End Sub


'==============================================================================
' SUBRUTINA AUXILIAR: CrearHojaFiltrada
'==============================================================================

Sub CrearHojaFiltrada(wb As Workbook, sHojaOrigen As String, sHojaDest As String, _
                      colsArray As Variant, filaHeader As Long)

    Dim wsOrigen As Worksheet
    Dim wsDestino As Worksheet
    Dim i As Long, j As Long, k As Long
    Dim ultimaFila As Long, ultimaColOrigen As Long
    Dim colIndices() As Long
    Dim numCols As Long
    Dim encontrado As Boolean
    Dim colDestino As Long
    Dim filaDestino As Long

    On Error Resume Next
    Set wsOrigen = wb.Sheets(sHojaOrigen)
    On Error GoTo 0

    If wsOrigen Is Nothing Then
        MsgBox "No se encontro la hoja: " & sHojaOrigen, vbCritical
        Exit Sub
    End If

    Application.DisplayAlerts = False
    On Error Resume Next
    wb.Sheets(sHojaDest).Delete
    On Error GoTo 0
    Application.DisplayAlerts = True

    Set wsDestino = wb.Sheets.Add(After:=wb.Sheets(wb.Sheets.Count))
    wsDestino.Name = sHojaDest

    ultimaFila = wsOrigen.Cells(wsOrigen.Rows.Count, 1).End(xlUp).Row
    ultimaColOrigen = wsOrigen.Cells(filaHeader, wsOrigen.Columns.Count).End(xlToLeft).Column

    numCols = UBound(colsArray) - LBound(colsArray) + 1
    ReDim colIndices(0 To numCols - 1)

    For i = 0 To numCols - 1
        encontrado = False
        For j = 1 To ultimaColOrigen
            If Trim(wsOrigen.Cells(filaHeader, j).Value) = Trim(colsArray(i)) Then
                colIndices(i) = j
                encontrado = True
                Exit For
            End If
        Next j
        If Not encontrado Then
            colIndices(i) = 0
        End If
    Next i

    colDestino = 1
    For i = 0 To numCols - 1
        If colIndices(i) > 0 Then
            wsDestino.Cells(1, colDestino).Value = wsOrigen.Cells(filaHeader, colIndices(i)).Value
            With wsDestino.Cells(1, colDestino)
                .Font.Bold = True
                .Font.Color = RGB(255, 255, 255)
                .Interior.Color = RGB(31, 73, 125)
            End With
            colDestino = colDestino + 1
        End If
    Next i

    filaDestino = 2

    For k = filaHeader + 1 To ultimaFila
        If Application.WorksheetFunction.CountA(wsOrigen.Rows(k)) > 0 Then
            colDestino = 1
            For i = 0 To numCols - 1
                If colIndices(i) > 0 Then
                    wsDestino.Cells(filaDestino, colDestino).Value = _
                        wsOrigen.Cells(k, colIndices(i)).Value
                    colDestino = colDestino + 1
                End If
            Next i
            filaDestino = filaDestino + 1
        End If
    Next k

    wsDestino.Columns.AutoFit
    wsDestino.Rows(1).AutoFilter

    wsDestino.Activate
    wsDestino.Rows(2).Select
    ActiveWindow.FreezePanes = True
    wsOrigen.Activate

End Sub
