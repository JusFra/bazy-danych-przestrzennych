# autor: Justyna Frankiewicz
# data wykonania: 14.12.2021
# automatyzacja przetwarzania

$NUMERINDEKSU = 287099
$TIMESTAMP = Get-Date -UFormat "%m%d%Y"
$source = 'https://home.agh.edu.pl/~wsarlej/Customers_Nov2021.zip'
$name = 'Customers_Nov2021'
$nazwa_log = "$($name)_raport_$TIMESTAMP"
$zipFilePassword = "agh"
$MyServer = "127.0.0.1"
$MyPort  = "5432"
$MyDB = "nowa"
$MyUid = "postgres"
$MyPass = "1024"

Set-Location -Path D:\geoinformatyka\Vsemestr\bazy_danych\cw8_9
$filepath = Resolve-Path "D:\geoinformatyka\Vsemestr\bazy_danych\cw8_9/"

# utworzenie pliku log w którym będą zapisywane wszystkie zakończone sukcesem procesy
$log = ".\PROCESSED\$nazwa_log.log"
Write-Output "Logfile $($TIMESTAMP)" >> $log

# pobieranie pliku
$destination = ".\$($name).zip"
Invoke-RestMethod -Uri $source -OutFile $destination
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały pobrane"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały pobrane - błąd!"
}

# rozpakownie pliku .zip
$7ZipPath = '"C:\Program Files\7-Zip\7z.exe"'
$command = "& $7ZipPath e -o$($filepath) -y -tzip -p$zipFilePassword $destination"
iex $command
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały rozpakowane"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały rozpakowane - błąd!"
}
$len = (Get-Content .\$($name).csv).Length - 1
Remove-Item .\$($name).zip


# czyszczenie pustych lini w pliku .csv
import-csv -path .\$($name).csv | Where-Object { $_.PSObject.Properties.Value -ne '' } | Export-Csv -Path ".\$($name)_clean.csv" -NoTypeInformation
Remove-Item .\$($name).csv


# porównywanie pliku Customers_Nov2021.csv z Customers_old.csv
$file1 = import-csv -path .\$($name)_clean.csv
$file2 = import-csv -path .\Customers_old.csv

foreach ($order1 in $file1){
    $a = $false
    foreach ($order2 in $file2){
        $obj = "" | select "first_name","last_name","email","lat","long"
        if($order2.'email' -eq $order1.'email' ){
            $a = $true
            $obj.'first_name' = $order1.'first_name'
            $obj.'last_name' = $order1.'last_name'
            $obj.'email' = $order1.'email'
            $obj.'lat' = $order1.'lat'
            $obj.'long' = $order1.'long'
        }
        
        $obj | Export-Csv -Path .\double.csv -Append -NoTypeInformation
        
    }
    $obj2 = "" | select "first_name","last_name","email","lat","long"
    if($a -eq $false){
            $obj2.'first_name' = $order1.'first_name'
            $obj2.'last_name' = $order1.'last_name'
            $obj2.'email' = $order1.'email'
            $obj2.'lat' = $order1.'lat'
            $obj2.'long' = $order1.'long'
            }
    $obj2 | Export-Csv -Path .\other.csv -Append -NoTypeInformation
       
}

If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały sprawdzone"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały sprawdzone - błąd!"
}

import-csv -path .\double.csv | Where-Object { $_.PSObject.Properties.Value -ne '' } | Export-Csv -Path ".\$($name).bad_$TIMESTAMP.csv" -NoTypeInformation
Remove-Item .\double.csv
import-csv -path .\other.csv | sort email,email –Unique | Where-Object { $_.PSObject.Properties.Value -ne '' } | Export-Csv -Path ".\$($name)_clean_new.csv" -NoTypeInformation
Remove-Item .\other.csv
Remove-Item .\$($name)_clean.csv
$len_clean = (Get-Content .\$($name)_clean_new.csv).Length - 1
$len_d = (Get-Content .\$($name).bad_$TIMESTAMP.csv).Length - 1
$len_t = (Get-Content .\$($name)_clean_new.csv).Length - 1



# w bazie danych PostgreSQL tworzymy tabelę CUSTOMERS_${NUMERINDEKSU} tylko jeśli taka tabela już nie istnieje (warunek if not exists)
# Install-Module PostgreSQLCmdlets

$DBConnectionString = "Driver={PostgreSQL UNICODE(x64)};Server=$MyServer;Port=$MyPort;Database=$MyDB;Uid=$MyUid;Pwd=$MyPass;"
$DBConn = New-Object System.Data.Odbc.OdbcConnection;
$DBConn.ConnectionString = $DBConnectionString;
$DBConn.Open();
$DBCmd = $DBConn.CreateCommand();
$DBCmd.CommandText = "CREATE TABLE IF NOT EXISTS CUSTOMERS_$($NUMERINDEKSU) (first_name VARCHAR,last_name VARCHAR,email VARCHAR,lat FLOAT,long FLOAT);";
$DBCmd.ExecuteReader();
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") tabela została storzona"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") tabela została storzona - błąd!"
}


# ładowanie danych ze zweryfikowanego pliku do tabeli CUSTOMERS_${NUMERINDEKSU}
$DBCmd2 = $DBConn.CreateCommand();
$DBCmd2.CommandText = "COPY CUSTOMERS_$($NUMERINDEKSU)(first_name, last_name, email, lat, long)
FROM '$filepath/Customers_Nov2021_clean_new.csv'
DELIMITER ','
CSV HEADER;";
$DBCmd2.ExecuteReader();
$DBConn.Close();
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały załadowane do tabeli"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane zostały załadowane do tabeli - błąd!"
}

# przeniesie przetworzony plik do podkatalogu PROCESSED dodając prefix ${TIMESTAMP}_ do nazwy pliku

$path = '.\PROCESSED'
If(!(test-path $path))
{
      New-Item -ItemType Directory -Force -Path $path
}

Move-Item -Path .\$($name)_clean_new.csv -Destination ./PROCESSED/$($TIMESTAMP)$($name).csv -PassThru
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") przetworzony plik został przeniesiony do podkatalogu PROCESSED"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") przetworzony plik został przeniesiony do podkatalogu PROCESSED - błąd!"
}


# wysyłamy email zawierający nst. raport: temat: CUSTOMERS LOAD - ${TIMESTAMP}, treść:
    # · liczba wierszy w pliku pobranym z internetu,
    # · liczba poprawnych wierszy (po czyszczeniu),
    # · liczba duplikatów w pliku wejściowym,
    # · ilość danych załadowanych do tabeli CUSTOMERS_${NUMERINDEKSU}.

$MyEmail = "justyna.test987@gmail.com"
$SMTP= "smtp.gmail.com"
$To = "justyna.test987@gmail.com"
$Subject = "CUSTOMERS LOAD - $($TIMESTAMP)"
$Body = "liczba wierszy w pliku pobranym z internetu: $($len),
liczba poprawnych wierszy (po czyszczeniu): $($len_clean),
liczba duplikatow w pliku wejsciowym: $($len_d),
ilosc danych zaladowanych do tabeli CUSTOMERS_$($NUMERINDEKSU): $($len_t)."
$Creds = (Get-Credential -Credential "$MyEmail")

Send-MailMessage -To $to -From $MyEmail -Subject $Subject -Body $Body -SmtpServer $SMTP -Credential $Creds -UseSsl -Port 587 -DeliveryNotificationOption never

If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") mail został wysłany"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") mail został wysłany - błąd!"
}


# uruchomienie kwerendy SQL, która znajdzie imiona i nazwiska klientów, którzy mieszkają w promieniu 50 kilometrów od punktu: 41.39988501005976, -75.67329768604034 
# (funkcja ST_DistanceSpheroid) i zapisze je do tabeli BEST_CUSTOMERS_${NUMERINDEKSU}

$DBConnectionString = "Driver={PostgreSQL UNICODE(x64)};Server=$MyServer;Port=$MyPort;Database=$MyDB;Uid=$MyUid;Pwd=$MyPass;"
$DBConn = New-Object System.Data.Odbc.OdbcConnection;
$DBConn.ConnectionString = $DBConnectionString;
$DBConn.Open();
$DBCmd = $DBConn.CreateCommand();
$DBCmd.CommandText = "CREATE TABLE IF NOT EXISTS BEST_CUSTOMERS_$($NUMERINDEKSU) AS
SELECT first_name, last_name FROM customers_287099
WHERE st_distancespheroid(ST_SetSRID(ST_MakePoint(lat, long),4326),ST_GeomFromText('POINT(41.39988501005976 -75.67329768604034)', 4326), 'SPHEROID[""WGS84"",6378137,298.257223563]') < 50000";
$DBCmd.ExecuteReader();
$DBConn.Close();
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") tabela BEST_CUSTOMERS została utworzona"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") tabela BEST_CUSTOMERS została utworzona - błąd!"
}


# eksport zawartości tabeli BEST_CUSTOMERS_${NUMERINDEKSU} do pliku csv o takiej samej nazwie jak tabela źródłowa
$DBConnectionString = "Driver={PostgreSQL UNICODE(x64)};Server=$MyServer;Port=$MyPort;Database=$MyDB;Uid=$MyUid;Pwd=$MyPass;"
$DBConn = New-Object System.Data.Odbc.OdbcConnection;
$DBConn.ConnectionString = $DBConnectionString;
$DBConn.Open();
$DBCmd = $DBConn.CreateCommand();
$DBCmd.CommandText = "COPY BEST_CUSTOMERS_$NUMERINDEKSU TO '$filepath\BEST_CUSTOMERS_$NUMERINDEKSU.csv' DELIMITER ',' CSV HEADER;";
$DBCmd.ExecuteReader();
$DBConn.Close();
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane z BEST_CUSTOMERS zostały eksportowane do .csv"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") dane z BEST_CUSTOMERS zostały eksportowane do .csv - błąd!"
}
$last_m = Get-Date -Format "MM/dd/yyyy HH:mm"
$l = (Get-Content .\BEST_CUSTOMERS_$NUMERINDEKSU.csv).Length - 1

# kompresja wyeksportowanego pliku csv:
Compress-Archive -Path .\BEST_CUSTOMERS_$NUMERINDEKSU.csv -DestinationPath .\BEST_CUSTOMERS_$($NUMERINDEKSU).zip
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") plik .csv został skompresowany"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") plik .csv został skompresowany - błąd!"
}

# wyśle skompresowany plik do adresata poczty razem z raportem o treści: data ostatniej modyfikacji, ilość wierszy w pliku csv
$MyEmail = "justyna.test987@gmail.com"
$SMTP= "smtp.gmail.com"
$To = "justyna.test987@gmail.com"
$Subject = "BEST_CUSTOMERS - $TIMESTAMP"
$Body = "data ostatniej modyfikacji: $last_m,
ilosc wierszy w pliku csv: $l."
$Creds = (Get-Credential -Credential "$MyEmail")

Send-MailMessage -To $to -From $MyEmail -Subject $Subject -Body $Body -Attachments .\BEST_CUSTOMERS_$($NUMERINDEKSU).zip -SmtpServer $SMTP -Credential $Creds -UseSsl -Port 587 -DeliveryNotificationOption never
If ($?)
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") mail2 został wysłany"
}
Else
{
    Add-content $Log -value "$(Get-Date -Format "MM/dd/yyyy HH:mm") mail2 został wysłany - błąd!"
}
