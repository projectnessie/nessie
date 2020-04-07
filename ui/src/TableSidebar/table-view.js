import React, { useState, useEffect } from 'react';

export default function TableView(props) {
  const [table, setTable] = useState(props.table);
  useEffect(() => setTable(props.table), [props]);
  return (<div className={"mainbar"}>
    {table == null ? "" : JSON.stringify(table, undefined, 2)}
    </div>)
}
