with
---- Clean and transform invoice data ---  Purpose: Extracts invoice data, calculates revenue, and filters out invalid transactions.---
  invoice_data as (
  select InvoiceNo, CustomerID, Country, InvoiceDate, sum(Quantity * UnitPrice) as revenue
  from `tc-da-1.turing_data_analytics.rfm`
  -- One-year interval, exclude customers with no ids
  where InvoiceDate between '2010-12-01' and '2011-12-01' and CustomerID is not null
  group by InvoiceNo, CustomerID, Country, InvoiceDate
  -- Exclude returns and negative prices
  having revenue > 0),

---- Calculate frequency and monetary and last_date ---   Purpose: Computes frequency and monetary value for each customer. ---
  f_and_m as (
  select CustomerID, Country, max(InvoiceDate) as last_purchase_date, count(distinct InvoiceDate) as frequency, round(sum(revenue), 2) as monetary
  from invoice_data
  group by 1, 2 ),

---- Calculate recency --- Purpose: Calculates the recency of each customer's last purchase in days. ---
  r as (
  select *, date_diff(date('2011-12-01'), date(last_purchase_date), day) as recency
  from f_and_m ),

---- Calculate 4 quartiles 25%, 50%, 75%, 100% --- Purpose: Computes approximate quartiles for recency, monetary, and frequency ---
  percentiles as (
  select
    approx_quantiles(recency, 4) as recency,
    approx_quantiles(monetary, 4) as monetary,
    approx_quantiles(frequency, 4) as frequency
  from r ),

---- Calculate scores ------
  scores as (
  select
    *,
      ---- Calculate fm_score ------
    cast(round((f_score + m_score) / 2, 0) as int64) as fm_score
  from (
    select
      r.*,

      ---- r score ------
      case
        when r.recency <= percentiles.recency[offset(1)] then 4
        when r.recency <= percentiles.recency[offset(2)] then 3
        when r.recency <= percentiles.recency[offset(3)] then 2
        when r.recency <= percentiles.recency[offset(4)] then 1
      end as r_score,

      ---- f score ------
      case
        when r.frequency <= percentiles.frequency[offset(1)] then 1
        when r.frequency <= percentiles.frequency[offset(2)] then 2
        when r.frequency <= percentiles.frequency[offset(3)] then 3
        when r.frequency <= percentiles.frequency[offset(4)] then 4
      end as f_score,

      ---- m score ------
      case
        when r.monetary <= percentiles.monetary[offset(1)] then 1
        when r.monetary <= percentiles.monetary[offset(2)] then 2
        when r.monetary <= percentiles.monetary[offset(3)] then 3
        when r.monetary <= percentiles.monetary[offset(4)] then 4
      end as m_score
    from r, percentiles)),

---- RFM score --- Purpose: Combines r_score and fm_score to create a simplified RFM code for each customer. ---
rfm_score as (select *, concat(r_score, fm_score) as rfm from scores),
  
---- RFM segments --- Purpose: Maps customers to RFM segments based on their RFM scores. ---  
rfm_segments as (
  select CustomerID, Country, rfm,
    case 
      when rfm = '44' then "Champions"
      when rfm = '34' or rfm = '43' or rfm = '33' then "Loyal"
      when rfm = '42' or rfm = '41' or rfm = '32' or rfm = '31' then "Promising"
      when rfm = '24' then "At Risk"
      when rfm = '22' or rfm = '23' then "Customers Needing Attention"
      when rfm = '13' or rfm = '14' then "Can't Lose Them" 
      when rfm = '21' or rfm = '12' then "Hibernating" 
      when rfm = '11' then "Lost" 
    end as rfm_segment
  from rfm_score )
  
---- Detailed data --- Purpose: Joins the RFM data with the segmented data to output detailed RFM information for each customer. --- 
select r.CustomerID, r.Country, r.recency, r.frequency, r.monetary, rfm_segment
from r
join rfm_segments s on r.CustomerID = s.CustomerID and r.Country = s.Country;

-- select * from invoice_data i
-- join rfm_segments r on i.CustomerID = r.CustomerID and i.Country = r.Country
-- where i.CustomerID in (select CustomerID from f_and_m 
-- group by CustomerID
-- having count(CustomerID) > 1)