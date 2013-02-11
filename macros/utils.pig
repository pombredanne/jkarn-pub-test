DEFINE TOP_N(records, field, n, asc_or_desc)
RETURNS top_n_records {
    ordered         =   ORDER $records BY $field $asc_or_desc;
    $top_n_records  =   LIMIT ordered $n;
};
