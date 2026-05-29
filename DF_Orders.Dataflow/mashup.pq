[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "olist_orders_dataset_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared olist_orders_dataset = let
  Source = Csv.Document(Web.Contents("https://myunt-my.sharepoint.com/personal/pranithaseemalamudi_my_unt_edu/Documents/Apps/Microsoft Power Query/Uploaded Files/olist_orders_dataset.csv"), [Delimiter = ",", Columns = 8, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"order_id", type text}, {"customer_id", type text}, {"order_status", type text}, {"order_purchase_timestamp", type datetime}, {"order_approved_at", type datetime}, {"order_delivered_carrier_date", type datetime}, {"order_delivered_customer_date", type datetime}, {"order_estimated_delivery_date", type datetime}}),
  #"Inserted month" = Table.AddColumn(#"Changed column type", "Month", each Date.Month([order_approved_at]), type nullable number),
  #"Inserted day name" = Table.AddColumn(#"Inserted month", "Day name", each Date.DayOfWeekName([order_purchase_timestamp]), type nullable text)
in
  #"Inserted day name";
shared NavigationQuery = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true])
in
  Source;
shared olist_orders_dataset_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[lakehouseName = "StagingLakehouseForDataflows_20260504010028"]}[Data],
  TableNavigation = Navigation_2{[Id = "olist_orders_dataset", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
