const sql = require('mssql');

const config = {
    user: 'aplicaciones',
    password: 'appEMMM$14',
    server: '192.168.33.206', // Puedes usar 'localhost\\instance' para nombrar una instancia
    database: 'BD_GRUPOVALOR_DESA',
    options: {
        encrypt: true, // Usa esto si te conectas a una instancia de Azure
        trustServerCertificate: true // Cambia a false si estás en producción
    }
};

function isObject(variable) {
    return typeof variable === 'object' && variable !== null && !Array.isArray(variable);
}

class OlympicWinnersService {

    async getData(request, resultsCallback) {
        try {
            await sql.connect(config);
            
            // Primero, obtenemos los valores únicos para las columnas pivote
            const pivotValues = await this.getPivotValues(request.pivotCols);
            
            const SQL = this.buildSql(request, pivotValues);
            const result = await sql.query(SQL);

            const rowCount = this.getRowCount(request, result.recordset);
            let resultsForPage = this.cutResultsToPageSize(request, result.recordset);
            
            let pivotFields = [];
            if (request.pivotCols && request.pivotCols.length > 0) {
                resultsForPage = this.applyPivotCols(resultsForPage, request.pivotCols, request.valueCols, pivotValues);
                pivotFields = this.getPivotFields(resultsForPage, pivotValues, request.valueCols);
            }

            if(pivotFields.length === 0){
                pivotFields = null;
            }

            resultsCallback(resultsForPage, rowCount, pivotFields);
        } catch (error) {
            console.error(error);
        } finally {
            await sql.close();
        }
    }

    async getPivotValues(pivotCols) {
        if (!pivotCols || pivotCols.length === 0) return {};

        const distinctCols = pivotCols.reduce((prev, item) => prev.concat(item.field) , []).join(', ');
        const query = `SELECT DISTINCT ${distinctCols} FROM Financiero.ViewLoteDetalle`;
        const result = await sql.query(query);
        return result.recordset;

        // if (!pivotCols || pivotCols.length === 0) return {};

        // const pivotValues = {};
        // for (const pivotCol of pivotCols) {
        //     const query = `SELECT DISTINCT ${pivotCol.field} FROM Financiero.ViewLoteDetalle`;
        //     const result = await sql.query(query);
        //     pivotValues[pivotCol.field] = result.recordset.map(row => row[pivotCol.field]);
        // }
        // return pivotValues;
    }

    buildSql(request, pivotValues) {
        const selectSql = this.createSelectSql(request, pivotValues);
        const fromSql = ' FROM Financiero.ViewLoteDetalle ';
        const whereSql = this.createWhereSql(request);
        const orderBySql = this.createOrderBySql(request);
        const groupBySql = this.createGroupBySql(request);
        const limitSql = this.createLimitSql(request);

        const SQL = selectSql + fromSql + whereSql + groupBySql + orderBySql + limitSql;

        console.log('===================================================================')
        console.log(SQL);
        console.log('===================================================================')

        return SQL;
    }

    createSelectSql(request, pivotValues) {
        const rowGroupCols = request.rowGroupCols;
        const valueCols = request.valueCols;
        const groupKeys = request.groupKeys;
        const pivotCols = request.pivotCols;

        if (this.isDoingGrouping(rowGroupCols, groupKeys)) {
            const colsToSelect = [];

            const rowGroupCol = rowGroupCols[groupKeys.length];
            colsToSelect.push(rowGroupCol.field);

            // Agregar columnas pivote a la selección
            if (pivotCols && pivotCols.length > 0) {
                pivotCols.forEach(pivotCol => {
                    const values = pivotValues[pivotCol.field] || [];
                    values.forEach(value => {
                        valueCols.forEach(valueCol => {
                            colsToSelect.push(`${valueCol.aggFunc}(CASE WHEN ${pivotCol.field} = '${value}' THEN ${valueCol.field} ELSE 0 END) AS ${pivotCol.field}_${value}_${valueCol.field}`);
                        });
                    });
                });
            } else {
                valueCols.forEach(function (valueCol) {
                    colsToSelect.push(valueCol.aggFunc + '(' + valueCol.field + ') as ' + valueCol.field);
                });
            }

            return ' SELECT ' + colsToSelect.join(', ');
        }

        return ' SELECT *';
    }

    createFilterSql(key, item) {
        switch (item.filterType) {
            case 'text':
                return this.createTextFilterSql(key, item);
            case 'number':
                return this.createNumberFilterSql(key, item);
            default:
                console.log('unknown filter type: ' + item.filterType);
        }
    }

    createNumberFilterSql(key, item) {
        switch (item.type) {
            case 'equals':
                return key + ' = ' + item.filter;
            case 'notEqual':
                return key + ' != ' + item.filter;
            case 'greaterThan':
                return key + ' > ' + item.filter;
            case 'greaterThanOrEqual':
                return key + ' >= ' + item.filter;
            case 'lessThan':
                return key + ' < ' + item.filter;
            case 'lessThanOrEqual':
                return key + ' <= ' + item.filter;
            case 'inRange':
                return '(' + key + ' >= ' + item.filter + ' AND ' + key + ' <= ' + item.filterTo + ')';
            default:
                console.log('unknown number filter type: ' + item.type);
                return '1=1';
        }
    }

    createTextFilterSql(key, item) {
        switch (item.type) {
            case 'equals':
                return key + " = '" + item.filter + "'";
            case 'notEqual':
                return key + " != '" + item.filter + "'";
            case 'contains':
                return key + " LIKE '%" + item.filter + "%'";
            case 'notContains':
                return key + " NOT LIKE '%" + item.filter + "%'";
            case 'startsWith':
                return key + " LIKE '" + item.filter + "%'";
            case 'endsWith':
                return key + " LIKE '%" + item.filter + "'";
            default:
                console.log('unknown text filter type: ' + item.type);
                return '1=1';
        }
    }

    createWhereSql(request) {
        // debugger;
        const rowGroupCols = request.rowGroupCols;
        const groupKeys = request.groupKeys;
        const filterModel = request.filterModel;

        const that = this;
        let whereParts = [];

        if (groupKeys.length > 0) {
            groupKeys.forEach(function (key, index) {
                const colName = rowGroupCols[index].field;
                whereParts.push(colName + " = '" + key + "'")
            });
        }

        if (filterModel) {
            if(filterModel?.conditions?.length > 0 || filterModel?.colId?.length > 0){
                whereParts = whereParts.concat(that.createAdvancedFilterSql(filterModel));
            } else {
                whereParts = whereParts.concat(that.createBasicFilterSql(filterModel));
            }
        }

        if (whereParts.length > 0) {
            return ' WHERE (' + whereParts.join(') AND (') + ')';
        } else {
            return '';
        }
    }

    createBasicFilterSql(filterModel){
        const that = this;
        let whereParts = [];

        const keySet = Object.keys(filterModel);
        keySet.forEach(function (key) {
            const item = filterModel[key];
            
            if(item?.conditions?.length > 0) {
                let dama = [];
                item.conditions.forEach((ele)=> {
                    dama.push(that.createFilterSql(key, ele));    
                })
                whereParts.push(dama.join(' ' + item.operator + ' '));
            } else {
                whereParts.push(that.createFilterSql(key, item));
            }
        });

        return whereParts;
    }

    createAdvancedFilterSql(filterModel){

        if(isObject(filterModel)){
            return this.createFilterSql(filterModel.colId, filterModel);
        } else {
            const that = this;

            const filterType = filterModel?.filterType;
            const type = filterModel?.type;
            const conditions = filterModel?.conditions;
    
            let whereParts = [];

            conditions.forEach((ele)=> {
                if(ele?.conditions?.length > 0){
                    whereParts.push(that.createAdvancedFilterSql(ele));
                } else {
                    whereParts.push(that.createFilterSql(ele.colId, ele));
                }
            })
    
            return  '('+ whereParts.join(') ' + type + ' (') + ')';
        }
    }

    createGroupBySql(request) {
        const rowGroupCols = request.rowGroupCols;
        const groupKeys = request.groupKeys;

        if (this.isDoingGrouping(rowGroupCols, groupKeys)) {
            const colsToGroupBy = [];

            const rowGroupCol = rowGroupCols[groupKeys.length];
            colsToGroupBy.push(rowGroupCol.field);

            return ' GROUP BY ' + colsToGroupBy.join(', ');
        } else {
            // select all columns
            return '';
        }
    }

    createOrderBySql(request) {
        const rowGroupCols = request.rowGroupCols;
        const groupKeys = request.groupKeys;
        const sortModel = request.sortModel;

        const grouping = this.isDoingGrouping(rowGroupCols, groupKeys);

        const sortParts = [];
        if (sortModel) {

            const groupColIds =
                rowGroupCols.map(groupCol => groupCol.id)
                    .slice(0, groupKeys.length + 1);

            sortModel.forEach(function (item) {
                if (grouping && groupColIds.indexOf(item.colId) < 0) {
                    // ignore
                } else {
                    sortParts.push(item.colId + ' ' + item.sort);
                }
            });
        }

        if (sortParts.length > 0) {
            return ' ORDER BY ' + sortParts.join(', ');
        } else {
            return '';
        }
    }

    isDoingGrouping(rowGroupCols, groupKeys) {
        return rowGroupCols.length > groupKeys.length;
    }

    createLimitSql(request) {
        const startRow = request.startRow;
        const endRow = request.endRow;
        const pageSize = endRow - startRow;

        if (!request.sortModel || request.sortModel.length === 0) {
            return ' ORDER BY (SELECT NULL) OFFSET ' + startRow + ' ROWS FETCH NEXT ' + pageSize + ' ROWS ONLY';
        } else {
            return ' OFFSET ' + startRow + ' ROWS FETCH NEXT ' + pageSize + ' ROWS ONLY';
        }
    }

    getRowCount(request, results) {
        if (!results || results.length === 0) {
            return null;
        }
        const currentLastRow = request.startRow + results.length;
        return currentLastRow <= request.endRow ? currentLastRow : -1;
    }

    cutResultsToPageSize(request, results) {
        const pageSize = request.endRow - request.startRow;
        if (results && results.length > pageSize) {
            return results.splice(0, pageSize);
        } else {
            return results;
        }
    }

    applyPivotCols(results, pivotCols, valueCols, pivotValues) {
        return results.map(row => {
            const pivotedRow = Object.assign({}, row);

            pivotCols.forEach(pivotCol => {
                const values = pivotValues[pivotCol.field] || [];
                values.forEach(value => {
                    valueCols.forEach(valueCol => {
                        const pivotKey = `${pivotCol.field}_${value}_${valueCol.field}`;
                        const newKey = `${value}_${valueCol.field}`;
                        pivotedRow[newKey] = pivotedRow[pivotKey];
                        delete pivotedRow[pivotKey];
                    });
                });
            });
            return pivotedRow;
        });
    }

    getPivotFields(results, pivotValues, valueCols) {
        const pivotFields = [];
        console.log(pivotValues, valueCols, '_NOR_');
        Object.entries(pivotValues).forEach(([pivotField, values]) => {
            values.forEach(value => {
                valueCols.forEach(valueCol => {
                    // pivotFields.push({ pivotValue: value, measureField: valueCol.field });
                    pivotFields.push(`${value}_${valueCol.field}`);
                });
            });
        });
        return pivotFields;
    }
}


module.exports = OlympicWinnersService;


