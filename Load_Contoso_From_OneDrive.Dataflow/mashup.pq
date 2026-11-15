[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "5b6d80b6-785f-42fe-bec3-2fd4653e79a2"]}[Data]{[lakehouseId = "440b0c40-f81b-482c-8816-c3842d00eddf"]}[Data];
shared Verify_Contoso_Source_Files = let
  Source = SharePoint.Files("https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "contoso-clean-source"))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared customer = let
  Source = Verify_Contoso_Source_Files,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name] = "customer.csv")),
  Navigation = #"Filtered rows"{[Name = "customer.csv", #"Folder Path" = "https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com/Documents/contoso-clean-source/"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 24, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"CustomerKey", Int64.Type}, {"GeoAreaKey", Int64.Type}, {"StartDT", type date}, {"EndDT", type date}, {"Continent", type text}, {"Gender", type text}, {"Title", type text}, {"GivenName", type text}, {"MiddleInitial", type text}, {"Surname", type text}, {"StreetAddress", type text}, {"City", type text}, {"State", type text}, {"StateFull", type text}, {"ZipCode", type text}, {"Country", type text}, {"CountryFull", type text}, {"Birthday", type date}, {"Age", Int64.Type}, {"Occupation", type text}, {"Company", type text}, {"Vehicle", type text}, {"Latitude", type number}, {"Longitude", type number}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared store = let
  Source = Verify_Contoso_Source_Files,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name] = "store.csv")),
  Navigation = #"Filtered rows"{[Name = "store.csv", #"Folder Path" = "https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com/Documents/contoso-clean-source/"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 11, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"StoreKey", Int64.Type}, {"StoreCode", Int64.Type}, {"GeoAreaKey", Int64.Type}, {"CountryCode", type text}, {"CountryName", type text}, {"State", type text}, {"OpenDate", type date}, {"CloseDate", type date}, {"Description", type text}, {"SquareMeters", Int64.Type}, {"Status", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared product = let
  Source = Verify_Contoso_Source_Files,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name] = "product.csv")),
  Navigation = #"Filtered rows"{[Name = "product.csv", #"Folder Path" = "https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com/Documents/contoso-clean-source/"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 14, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"ProductKey", Int64.Type}, {"ProductCode", Int64.Type}, {"ProductName", type text}, {"Manufacturer", type text}, {"Brand", type text}, {"Color", type text}, {"WeightUnit", type text}, {"Weight", type number}, {"Cost", type number}, {"Price", type number}, {"CategoryKey", Int64.Type}, {"CategoryName", type text}, {"SubCategoryKey", Int64.Type}, {"SubCategoryName", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared date = let
  Source = Verify_Contoso_Source_Files,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name] = "date.csv")),
  Navigation = #"Filtered rows"{[Name = "date.csv", #"Folder Path" = "https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com/Documents/contoso-clean-source/"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 17, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Date", type date}, {"DateKey", Int64.Type}, {"Year", Int64.Type}, {"YearQuarter", type text}, {"YearQuarterNumber", Int64.Type}, {"Quarter", type text}, {"YearMonth", type date}, {"YearMonthShort", type date}, {"YearMonthNumber", Int64.Type}, {"Month", type text}, {"MonthShort", type text}, {"MonthNumber", Int64.Type}, {"DayofWeek", type text}, {"DayofWeekShort", type text}, {"DayofWeekNumber", Int64.Type}, {"WorkingDay", Int64.Type}, {"WorkingDayNumber", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared currencyexchange = let
  Source = Verify_Contoso_Source_Files,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name] = "currencyexchange.csv")),
  Navigation = #"Filtered rows"{[Name = "currencyexchange.csv", #"Folder Path" = "https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com/Documents/contoso-clean-source/"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 4, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Date", type date}, {"FromCurrency", type text}, {"ToCurrency", type text}, {"Exchange", type number}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared sales = let
  Source = Verify_Contoso_Source_Files,
  #"Filtered rows" = Table.SelectRows(Source, each ([Name] = "sales.csv")),
  Navigation = #"Filtered rows"{[Name = "sales.csv", #"Folder Path" = "https://ksoszka-my.sharepoint.com/personal/kamil_soszka_ksoszka_onmicrosoft_com/Documents/contoso-clean-source/"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = ",", Columns = 13, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"OrderKey", Int64.Type}, {"LineNumber", Int64.Type}, {"OrderDate", type date}, {"DeliveryDate", type date}, {"CustomerKey", Int64.Type}, {"StoreKey", Int64.Type}, {"ProductKey", Int64.Type}, {"Quantity", Int64.Type}, {"UnitPrice", type number}, {"NetPrice", type number}, {"UnitCost", type number}, {"CurrencyCode", type text}, {"ExchangeRate", type number}})
in
  #"Changed column type";
