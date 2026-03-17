'==============================================================================
' MACRO: Crear_Hojas_Filtradas
' DESCRIPCION: Crea dos nuevas hojas ("Ventas_DB" y "VentasDetallado_DB")
'              copiando solo las columnas seleccionadas desde las hojas
'              "Ventas" y "Ventas Detallado", a partir de la fila 6 (cabecera).
'              Adicionalmente agrega la columna calculada "Factura" en ambas hojas.
' INSTRUCCIONES:
'   1. Abrir el archivo "Reporte de Ventas.xlsx"
'   2. Presionar Alt + F11 para abrir el Editor de VBA
'   3. Ir a Insertar > Modulo
'   4. Pegar todo este codigo en el modulo
'   5. Cerrar el editor y volver a Excel
'   6. Presionar Alt + F8, seleccionar "Crear_Hojas_Filtradas" y ejecutar
'   (Opcional) Asignar la macro a un boton en la hoja para ejecucion rapida
'==============================================================================

Sub Crear_Hojas_Filtradas()

    Dim wb As Workbook
    Set wb = ThisWorkbook

    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual

    ' -------------------------------------------------------------------------
    ' HOJA 1: Ventas -> Ventas_DB
    ' -------------------------------------------------------------------------
    Dim parte1V As Variant, parte2V As Variant
    Dim colsVentas As Variant
    Dim i As Long, idx As Long

    parte1V = Array("Codigo Venta", "Ejercicio", "Periodo", "Registro Ctb.", _
                    "Fecha Mov.", "Tipo Doc.", "Serie", "Numero", _
                    "TDI Cliente", "Doc. Cliente", "Cliente", _
                    "Fecha Emi.", "Fecha Vnc.", "Forma Pago", _
                    "Cod. Moneda", "Moneda", "Tipo Cambio")

    parte2V = Array("TDI Vendedor", "Doc. Vendedor", "Vendedor", "Anulado", _
                    "Valor", "Descuento", "Valor Afe.", "Valor Ina.", "Igv", "Total", _
                    "Retenci" & Chr(243) & "n", "Total Venta", "Observacion", _
                    "Doc.Ref. Fecha", "Doc.Ref. Tipo Doc.", "Doc.Ref. Serie", "Doc.Ref. Numero", _
                    "Doc. Elect.", "Doc. Elect. Envio Sunat", "Doc. Elect. Descrip. Resp.", _
                    "Fecha Reg.", "Usuario Reg.")

    ReDim colsVentas(0 To (UBound(parte1V) + 1) + (UBound(parte2V) + 1) - 1)
    idx = 0
    For i = 0 To UBound(parte1V): colsVentas(idx) = parte1V(i): idx = idx + 1: Next i
    For i = 0 To UBound(parte2V): colsVentas(idx) = parte2V(i): idx = idx + 1: Next i

    Call CrearHojaFiltrada(wb, "Ventas", "Ventas_DB", colsVentas, 6)

    ' --- Agregar columna "Factura" en Ventas_DB (Serie & "-" & Numero) -----------
    Call AgregarColumnaFacturaVentas(wb, "Ventas_DB")

    ' -------------------------------------------------------------------------
    ' HOJA 2: Ventas Detallado -> VentasDetallado_DB
    ' -------------------------------------------------------------------------
    Dim colsDetallado As Variant
    colsDetallado = Array("Codigo Venta", "Codigo", "Producto / Servicio", _
                          "Cant", "Valor Unitario", "Importe", "IGV", "Total Neto")

    Call CrearHojaFiltrada(wb, "Ventas Detallado", "VentasDetallado_DB", colsDetallado, 6)

    ' --- Agregar columna "Factura" en VentasDetallado_DB (BUSCARV desde Ventas_DB) ---
    Call AgregarColumnaFacturaDetallado(wb, "VentasDetallado_DB", "Ventas_DB")

    ' --- Agregar columna "Clave_Inventario" en VentasDetallado_DB (Factura & Codigo & Cant) ---
    Call AgregarClaveInventario(wb, "VentasDetallado_DB")

    ' -------------------------------------------------------------------------
    ' Finalizar
    ' -------------------------------------------------------------------------
    Application.ScreenUpdating = True
    Application.Calculation = xlCalculationAutomatic

    MsgBox "Proceso completado exitosamente." & vbCrLf & vbCrLf & _
           "Se crearon/actualizaron las hojas:" & vbCrLf & _
           "  - Ventas_DB (con columna Factura)" & vbCrLf & _
           "  - VentasDetallado_DB (con columna Factura y Clave_Inventario)", vbInformation, "Hojas Filtradas"

End Sub


'==============================================================================
' SUBRUTINA: AgregarColumnaFacturaVentas
' Agrega al final de Ventas_DB la columna "Factura" = Serie & "-" & Numero
'==============================================================================

Sub AgregarColumnaFacturaVentas(wb As Workbook, sHoja As String)

    Dim ws As Worksheet
    Dim ultimaCol As Long, ultimaFila As Long
    Dim colSerie As Long, colNumero As Long
    Dim i As Long, j As Long

    Set ws = wb.Sheets(sHoja)

    ultimaFila = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    ultimaCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    ' Buscar las columnas Serie y Numero en la fila de encabezados (fila 1)
    colSerie = 0
    colNumero = 0
    For j = 1 To ultimaCol
        If Trim(ws.Cells(1, j).Value) = "Serie" Then colSerie = j
        If Trim(ws.Cells(1, j).Value) = "Numero" Then colNumero = j
    Next j

    If colSerie = 0 Or colNumero = 0 Then
        MsgBox "No se encontraron las columnas Serie o Numero en " & sHoja, vbExclamation
        Exit Sub
    End If

    ' Agregar encabezado "Factura" en la siguiente columna disponible
    Dim colFactura As Long
    colFactura = ultimaCol + 1

    With ws.Cells(1, colFactura)
        .Value = "Factura"
        .Font.Bold = True
        .Font.Color = RGB(255, 255, 255)
        .Interior.Color = RGB(31, 73, 125)
    End With

    ' Rellenar Factura = Serie & "-" & Numero para cada fila de datos
    For i = 2 To ultimaFila
        If ws.Cells(i, 1).Value <> "" Then
            ws.Cells(i, colFactura).Value = ws.Cells(i, colSerie).Value & _
                                            "-" & ws.Cells(i, colNumero).Value
        End If
    Next i

    ws.Columns(colFactura).AutoFit

End Sub


'==============================================================================
' SUBRUTINA: AgregarColumnaFacturaDetallado
' Agrega en VentasDetallado_DB la columna "Factura" buscando el Codigo Venta
' en Ventas_DB y trayendo el valor de su columna "Factura"
'==============================================================================

Sub AgregarColumnaFacturaDetallado(wb As Workbook, sHojaDetallado As String, sHojaVentas As String)

    Dim wsDetallado As Worksheet
    Dim wsVentas As Worksheet
    Dim ultimaColDet As Long, ultimaFilaDet As Long
    Dim ultimaFilaVen As Long
    Dim colCodVentaDet As Long, colCodVentaVen As Long, colFacturaVen As Long
    Dim i As Long, j As Long
    Dim codigoVenta As String
    Dim facturaEncontrada As String
    Dim encontrado As Boolean

    Set wsDetallado = wb.Sheets(sHojaDetallado)
    Set wsVentas = wb.Sheets(sHojaVentas)

    ultimaFilaDet = wsDetallado.Cells(wsDetallado.Rows.Count, 1).End(xlUp).Row
    ultimaFilaVen = wsVentas.Cells(wsVentas.Rows.Count, 1).End(xlUp).Row
    ultimaColDet = wsDetallado.Cells(1, wsDetallado.Columns.Count).End(xlToLeft).Column

    ' Buscar columna "Codigo Venta" en VentasDetallado_DB
    colCodVentaDet = 0
    For j = 1 To ultimaColDet
        If Trim(wsDetallado.Cells(1, j).Value) = "Codigo Venta" Then
            colCodVentaDet = j
            Exit For
        End If
    Next j

    ' Buscar columnas "Codigo Venta" y "Factura" en Ventas_DB
    Dim ultimaColVen As Long
    ultimaColVen = wsVentas.Cells(1, wsVentas.Columns.Count).End(xlToLeft).Column
    colCodVentaVen = 0
    colFacturaVen = 0
    For j = 1 To ultimaColVen
        If Trim(wsVentas.Cells(1, j).Value) = "Codigo Venta" Then colCodVentaVen = j
        If Trim(wsVentas.Cells(1, j).Value) = "Factura" Then colFacturaVen = j
    Next j

    If colCodVentaDet = 0 Or colCodVentaVen = 0 Or colFacturaVen = 0 Then
        MsgBox "No se encontraron las columnas necesarias para el BUSCARV de Factura.", vbExclamation
        Exit Sub
    End If

    ' Agregar encabezado "Factura" al final de VentasDetallado_DB
    Dim colFacturaDet As Long
    colFacturaDet = ultimaColDet + 1

    With wsDetallado.Cells(1, colFacturaDet)
        .Value = "Factura"
        .Font.Bold = True
        .Font.Color = RGB(255, 255, 255)
        .Interior.Color = RGB(31, 73, 125)
    End With

    ' Para cada fila de VentasDetallado_DB, buscar el Codigo Venta en Ventas_DB
    ' y traer el valor de la columna Factura (equivalente a un BUSCARV)
    For i = 2 To ultimaFilaDet
        codigoVenta = CStr(wsDetallado.Cells(i, colCodVentaDet).Value)
        If codigoVenta <> "" Then
            encontrado = False
            For j = 2 To ultimaFilaVen
                If CStr(wsVentas.Cells(j, colCodVentaVen).Value) = codigoVenta Then
                    wsDetallado.Cells(i, colFacturaDet).Value = wsVentas.Cells(j, colFacturaVen).Value
                    encontrado = True
                    Exit For
                End If
            Next j
            If Not encontrado Then
                wsDetallado.Cells(i, colFacturaDet).Value = "No encontrado"
            End If
        End If
    Next i

    wsDetallado.Columns(colFacturaDet).AutoFit

End Sub



'==============================================================================
' SUBRUTINA: AgregarClaveInventario
' Agrega en VentasDetallado_DB la columna "Clave_Inventario"
' formada por: Factura & Codigo & Cant (concatenados sin separador)
'==============================================================================

Sub AgregarClaveInventario(wb As Workbook, sHoja As String)

    Dim ws As Worksheet
    Dim ultimaCol As Long, ultimaFila As Long
    Dim colFactura As Long, colCodigo As Long, colCant As Long
    Dim i As Long, j As Long

    Set ws = wb.Sheets(sHoja)

    ultimaFila = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row
    ultimaCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    ' Buscar las columnas Factura, Codigo y Cant en la fila de encabezados (fila 1)
    colFactura = 0
    colCodigo = 0
    colCant = 0
    For j = 1 To ultimaCol
        Select Case Trim(ws.Cells(1, j).Value)
            Case "Factura": colFactura = j
            Case "Codigo":  colCodigo = j
            Case "Cant":    colCant = j
        End Select
    Next j

    If colFactura = 0 Or colCodigo = 0 Or colCant = 0 Then
        MsgBox "No se encontraron todas las columnas necesarias para Clave_Inventario." & _
               vbCrLf & "(Factura, Codigo, Cant)", vbExclamation
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

    ' Rellenar Clave_Inventario = Factura & Codigo & Cant (sin decimales) para cada fila de datos
    For i = 2 To ultimaFila
        If ws.Cells(i, 1).Value <> "" Then
            ws.Cells(i, colClave).Value = CStr(ws.Cells(i, colFactura).Value) & _
                                          CStr(ws.Cells(i, colCodigo).Value) & _
                                          CStr(CLng(ws.Cells(i, colCant).Value))
        End If
    Next i

    ws.Columns(colClave).AutoFit

End Sub


'==============================================================================
' SUBRUTINA AUXILIAR: CrearHojaFiltrada
' Parametros:
'   wb          -> Libro de trabajo
'   sHojaOrigen -> Nombre de la hoja origen
'   sHojaDest   -> Nombre de la hoja destino a crear/reemplazar
'   colsArray   -> Array con los nombres de columnas a copiar
'   filaHeader  -> Numero de fila donde estan los encabezados (ej: 6)
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
            Debug.Print "Columna no encontrada en '" & sHojaOrigen & "': " & colsArray(i)
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

    Debug.Print "Hoja '" & sHojaDest & "' creada con " & (filaDestino - 2) & " filas de datos."

End Sub
