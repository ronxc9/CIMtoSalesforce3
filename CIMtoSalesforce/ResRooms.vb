Imports System.Data.Odbc
Imports System.Data
Imports System.IO.Directory


Public Class ResRooms

    Dim qry As String
    Dim connString As String
    Dim connODBC As OdbcConnection
    Dim cmdODBC As OdbcCommand
    Dim daODBC As OdbcDataAdapter
    Dim dt As DataTable
    Dim csvString As String
    Dim UTFStream As Object


    Public Sub New()
        PrepData()
        BuildString()
        DumpData()
    End Sub

    Private Sub PrepData()
        Dim qry As String
        qry = My.Computer.FileSystem.ReadAllText("\\rh-util02\SalesforceCSV\Application\Query.txt")

        '        qry = "/* RESIDENT ROOM ASSIGNMENTS*/" & vbCrLf &
        ' "SELECT re_master.resident_no, re_master_subsidy.room_area, CONVERT(INT, re_master_subsidy.room_no) AS room_no, re_master_subsidy.costcenter, re_master_subsidy.type, re_master_subsidy.location, re_master_subsidy.bed_no, re_master.active" & vbCrLf &
        ' "INTO #temp" & vbCrLf &
        ' "FROM re_master_subsidy  JOIN re_master ON re_master.resident_no= re_master_subsidy.resident_no                                                     " & vbCrLf &
        ' "WHERE  re_master_subsidy.last_entered_record='Y' AND re_master.active='Y' AND re_master_subsidy.costcenter IN ('HOS', 'NH', 'ILU') AND re_master_subsidy.type<>'D' " & vbCrLf &
        ' "ORDER BY re_master_subsidy.location, room_area, re_master_subsidy.room_no" & vbCrLf &
        ' "" & vbCrLf &
        ' "" & vbCrLf &
        ' "SELECT re_location.code AS location_code,re_location.description AS location_description,  re_room.costcenter, re_costcenter.description AS cost_center_description,  re_room.area,  um_unit_type.description AS 'Room_name',um_master.unit_id, re_room.room_no, " & vbCrLf &
        ' "CASE um_master.status WHEN 'O' THEN 'Occupied' WHEN 'V' THEN 'Vacant' WHEN 'R' THEN 'In repair' END AS Status, " & vbCrLf &
        ' "re_room_price.description AS room_group, #temp.resident_no, #temp.bed_no, re_room.description AS room_description" & vbCrLf &
        ' "INTO #room_assign" & vbCrLf &
        ' "FROM um_master JOIN re_location  ON re_location.code = um_master.location" & vbCrLf &
        ' "                                       JOIN re_room ON um_master.unit_id= re_room.unit_id" & vbCrLf &
        ' "                                       JOIN re_costcenter ON re_costcenter.code= re_room.costcenter" & vbCrLf &
        ' "                                       LEFT OUTER JOIN um_unit_type ON  um_master.unit_type= um_unit_type.code" & vbCrLf &
        ' "                                       LEFT OUTER JOIN re_room_price ON  re_room_price.price_code= um_master.price_code       " & vbCrLf &
        ' "                                       LEFT OUTER  JOIN #temp ON LTRIM(RTRIM(#temp.location))= LTRIM(RTRIM(location_code)) AND CONVERT(INT, #temp.room_no)= CONVERT(INT, re_room.room_no) AND LTRIM(RTRIM(#temp.room_area))=LTRIM(RTRIM(re_room.area))" & vbCrLf &
        ' "GROUP BY re_location.description, area,  um_unit_type.description, re_location.code,  re_room_price.description, um_master.unit_id, re_room.room_no, um_master.market_value_date ,  re_room.costcenter, cost_center_description,  status, room_group, #temp.resident_no, #temp.bed_no,re_room.description" & vbCrLf &
        ' "ORDER BY location_code, area, re_room.room_no" & vbCrLf &
        ' "" & vbCrLf &
        ' "" & vbCrLf &
        ' "" & vbCrLf &
        ' "/*GET PERSON/RESIDENT details */" & vbCrLf &
        ' "" & vbCrLf &
        ' "Select sy_person_master.id_number" & vbCrLf &
        ' "INTO #temp5" & vbCrLf &
        ' "FROM sy_person_master" & vbCrLf &
        ' "WHERE sy_person_master.deceased<>'Y' AND sy_person_master.active='Y' AND sy_person_master.company='N' AND (sy_person_master.modified_date>=today() OR sy_person_master.created_date>=today())" & vbCrLf &
        ' "GROUP By sy_person_master.id_number" & vbCrLf &
        ' "UNION" & vbCrLf &
        ' "Select sy_person_master.id_number" & vbCrLf &
        ' "FROM cs_master JOIN sy_person_master ON sy_person_master.id_number= cs_master.person_id_number" & vbCrLf &
        ' "WHERE (cs_master.modified_date>=today()  OR cs_master.created_date>= today()) AND cs_master.active='Y'  AND sy_person_master.deceased<>'Y' AND sy_person_master.company='N' AND sy_person_master.active='Y'" & vbCrLf &
        ' "UNION" & vbCrLf &
        ' "Select sy_person_master.id_number" & vbCrLf &
        ' "FROM re_master JOIN sy_person_master ON sy_person_master.id_number= re_master.person_id_number" & vbCrLf &
        '                                    "LEFT OUTER JOIN #room_assign ON #room_assign.resident_no= re_master.resident_no" & vbCrLf &
        ' "WHERE (re_master.modified_date IS NULL OR re_master.modified_date >=today()) AND re_master.active='Y' AND re_master.location NOT IN ('ADM') AND re_master.costcenter NOT IN ('ADM')" & vbCrLf &
        ' "GROUP By sy_person_master.id_number" & vbCrLf &
        ' "" & vbCrLf &
        ' "" & vbCrLf &
        ' "" & vbCrLf &
        ' "" & vbCrLf &
        ' "SELECT sy_person_master.given_name_1 + sy_person_master.surname+ CONVERT(char(10),sy_person_master.dob,103)+'Resthaven' AS upsert, sy_person_master.id_number, sy_person_master.title, sy_person_master.surname, sy_person_master.given_name_1, sy_person_master.given_name_2, sy_person_master.preferred_name, sy_person_master.dob , CASE sy_person_master.estimated_dob WHEN 1 THEN 'Y' WHEN 2 THEN 'N' END AS estimated_dob, sy_person_master.sex, sy_person_master.address_street + ' ' + sy_person_master.address_street_2 AS address_street, sy_person_master.address_suburb, sy_person_master.address_postcode, sy_person_master.address_country, sy_person_master.address_state,  sy_person_master.home_phone_area_code +' ' + sy_person_master.home_phone AS home_phone, sy_person_master.moblie_phone, sy_person_master.email, sy_person_master.pension_type, sy_person_master.medicare_no, sy_person_master.medicare_expiry_date,   LIST(cs_master.program_code, ', ') AS Service_Summary_code, LIST(cs_program.description, ', ') AS Service_Summary_Description, re_master.resident_no, re_master.patient_id AS commonwealth_no, re_master.active, sy_person_master.pension_no, dva_card_no, sy_person_master.country_of_birth " & vbCrLf &
        ' "--INTO #person" & vbCrLf &
        ' "FROM sy_person_master  LEFT OUTER JOIN cs_master  ON sy_person_master.id_number= cs_master.person_id_number" & vbCrLf &
        ' "                                                      LEFT OUTER JOIN re_master ON sy_person_master.id_number= re_master.person_id_number " & vbCrLf &
        ' "                                                     LEFT OUTER JOIN cs_program ON cs_program.code= cs_master.program_code     " & vbCrLf &
        ' "WHERE (sy_person_master.id_number IN (SELECT id_number FROM #temp5) )  " & vbCrLf &
        '  "GROUP BY sy_person_master.id_number, sy_person_master.surname, sy_person_master.given_name_1, sy_person_master.preferred_name, sy_person_master.dob, sy_person_master.estimated_dob, sy_person_master.sex, sy_person_master.address_street, sy_person_master.address_street_2, sy_person_master.address_suburb, sy_person_master.address_postcode, sy_person_master.address_country, sy_person_master.home_phone_area_code, sy_person_master.home_phone, sy_person_master.moblie_phone, sy_person_master.email, sy_person_master.pension_type, sy_person_master.medicare_no, re_master.resident_no, re_master.patient_id, sy_person_master.title, address_state, sy_person_master.medicare_expiry_date, re_master.active, sy_person_master.given_name_2,  sy_person_master.pension_no, dva_card_no, sy_person_master.country_of_birth" & vbCrLf &
        '"HAVING (re_master.active='Y' OR re_master.active IS NULL)"

        connString = "DSN=finance"

        connODBC = New OdbcConnection(connString)
        cmdODBC = New OdbcCommand(qry, connODBC)
        daODBC = New OdbcDataAdapter(cmdODBC)
        dt = New DataTable

        Try

            daODBC.Fill(dt)

        Catch ex As OdbcException
            Debug.WriteLine("ODBC Error: " & ex.Message)
        Finally

        End Try
    End Sub


    Private Sub BuildString()
        csvString = ""
        Dim dc As System.Data.DataColumn
        Dim dr As System.Data.DataRow
        Dim colIndex As Integer = 0
        Dim rowIndex As Integer = 0

        'Export the Columns to excel file
        For Each dc In dt.Columns
            colIndex = colIndex + 1
            csvString = csvString & dc.ColumnName & ","
        Next
        csvString = csvString.Substring(0, csvString.Length) & vbNewLine

        'Export the rows to excel file
        For Each dr In dt.Rows
            rowIndex = rowIndex + 1
            colIndex = 0
            For Each dc In dt.Columns
                colIndex = colIndex + 1
                csvString = csvString & dr(dc.ColumnName).ToString().Replace(",", " ") & ","
                csvString = csvString.Replace("""", "'")
            Next
            csvString = csvString.Substring(0, csvString.Length) & vbNewLine
        Next
    End Sub

    Private Sub DumpData()

        Dim utf8WithoutBom As New System.Text.UTF8Encoding(False)
        Dim finalPath As String = "\\rh-util02\SalesforceCSV\CIMToSalesforce.csv"
        'Dim finalPath As String = "C:\Users\ron.rosario\Desktop\CIMToSalesforce.csv"

        Try
            My.Computer.FileSystem.WriteAllText(finalPath, csvString, False, utf8WithoutBom)

            'UTFStream = CreateObject("adodb.stream")


        Catch ex As IO.IOException
            Debug.WriteLine("Unable to write file: " & ex.Message)
        End Try



   

    End Sub


End Class

