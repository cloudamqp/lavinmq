import * as Pagination from './pagination.js';
import * as TableHeaderSort from './tableheadersort.js';
import { UrlDataSource } from './datasource.js';
export function renderTable(id, options = {}, renderRow) {
    const countId = options.countId ?? 'pagename-label';
    const dataSource = options.dataSource ?? new UrlDataSource(options.url ?? '');
    const table = document.getElementById(id);
    if (!table)
        throw new Error(`Table with id "${id}" not found`);
    const grandparent = table.parentElement?.parentElement;
    const tableHeader = grandparent ? grandparent.querySelector(':scope > .table-header') : null;
    const container = tableHeader || table.parentElement;
    const keyColumns = options.keyColumns ?? [];
    const events = new EventTarget();
    if (options.columnSelector) {
        renderColumnSelector(table);
    }
    if (options.search && container) {
        renderSearch(container, dataSource);
    }
    if (options.pagination) {
        const paginationCell = table.createTFoot().insertRow().insertCell();
        const paginationContainer = document.createElement('div');
        const headerRow = table.tHead?.rows[0];
        paginationCell.colSpan = headerRow ? headerRow.children.length : 1;
        paginationCell.appendChild(paginationContainer);
        paginationContainer.classList.add('pagination');
        Pagination.create(paginationContainer, dataSource);
    }
    if (table.querySelector('th[data-sort-key]')) {
        TableHeaderSort.create(table, dataSource);
    }
    dataSource.on('update', updateTable);
    dataSource.on('error', ((error) => {
        console.log(error);
        toggleDisplayError(id, 'Error fetching data: ' + error.detail);
    }));
    dataSource.reload();
    function on(event, args) {
        events.addEventListener(event, args);
    }
    function reload() {
        return dataSource.reload();
    }
    function updateTable() {
        const items = dataSource.items;
        const totalCount = dataSource.totalCount;
        const countEl = document.getElementById(countId);
        if (countEl)
            countEl.textContent = String(totalCount);
        const tableEl = document.getElementById(id);
        if (!tableEl)
            return;
        const t = tableEl.tBodies[0];
        if (!t)
            return;
        if (!Array.isArray(items) || items.length === 0) {
            t.textContent = '';
            const tr = t.appendChild(document.createElement('tr'));
            const td = tr.appendChild(document.createElement('td'));
            const headerRow = tableEl.tHead?.rows[0];
            td.colSpan = headerRow
                ? Array.from(headerRow.children).reduce((sum, item) => sum + item.colSpan, 0)
                : 1;
            td.classList.add('center');
            td.textContent = 'Nope, nothing to see here.';
            return;
        }
        toggleDisplayError(id, null);
        const rows = Array.from(t.rows);
        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            if (!item)
                continue;
            const currentRow = t.rows[i];
            try {
                const foundRow = findRow(rows, item);
                if (foundRow) {
                    // Item is display, update that row
                    renderRow(foundRow, item, false);
                    // And make sure the row is in the right place
                    if (foundRow !== currentRow) {
                        t.insertBefore(foundRow, currentRow ?? null);
                    }
                }
                else {
                    // New item, create new row
                    const tr = t.insertRow(i);
                    setKeyAttributes(tr, item);
                    renderRow(tr, item, true);
                }
            }
            catch (e) {
                const error = e;
                toggleDisplayError(id, item.error || error.message);
            }
        }
        let rowsToDelete = t.rows.length - items.length;
        while (rowsToDelete-- > 0) {
            t.deleteRow(t.rows.length - 1);
        }
        events.dispatchEvent(new CustomEvent('updated'));
    }
    function findRow(rows, item) {
        return rows.find((row) => keyColumns.every((key) => row.dataset[key] === JSON.stringify(item[key])));
    }
    function setKeyAttributes(tr, item) {
        keyColumns.forEach((key) => {
            tr.dataset[key] = JSON.stringify(item[key]);
        });
    }
    function renderSearch(cont, ds) {
        const form = document.createElement('form');
        form.classList.add('form');
        form.addEventListener('submit', (e) => {
            e.preventDefault();
        });
        const filterInput = document.createElement('input');
        filterInput.type = 'search';
        filterInput.classList.add('filter-table');
        filterInput.placeholder = 'Filter regex';
        if (cont.dataset['clearSearch'] === '1')
            ds.searchTerm = '';
        filterInput.value = ds.searchTerm ?? '';
        form.appendChild(filterInput);
        cont.insertBefore(form, cont.children[0] ?? null);
        let liveType;
        const apply = () => {
            ds.searchTerm = filterInput.value;
            ds.page = 1;
            clearTimeout(liveType);
            reload();
        };
        filterInput.addEventListener('input', () => {
            clearTimeout(liveType);
            liveType = setTimeout(apply, 500);
        });
        filterInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                apply();
            }
            else if (e.key === 'Escape') {
                if (filterInput.value) {
                    filterInput.value = '';
                    apply();
                }
            }
        });
        // Fires when the native clear "x" is clicked (because type="search")
        filterInput.addEventListener('search', () => {
            if (filterInput.value === '' && ds.searchTerm) {
                apply();
            }
        });
        ds.on('update', () => {
            if (filterInput !== document.activeElement) {
                filterInput.value = ds.searchTerm;
            }
        });
    }
    return { updateTable, reload, on };
}
export function renderCell(tr, column, value, classList = '') {
    const cell = tr.cells[column] || buildCells(tr, column);
    if (!cell)
        return undefined;
    if (value instanceof Element) {
        if (cell.firstChild) {
            cell.replaceChild(value, cell.firstChild);
        }
        else {
            cell.appendChild(value);
        }
    }
    else {
        const text = value == null ? '' : value.toString();
        if (cell.textContent !== text) {
            cell.textContent = text;
        }
    }
    if (cell.classList.contains('hide'))
        return undefined;
    cell.className = classList;
    return cell;
}
function buildCells(tr, index) {
    const target = index + 1;
    let i = index;
    while (i >= 0) {
        if (tr.cells.length >= target)
            break;
        tr.insertCell(-1);
        i--;
    }
    const tbl = tr.parentElement?.parentElement;
    if (!tbl)
        return undefined;
    const colHeader = tbl.querySelectorAll(`tr > *:nth-child(${target})`)[0];
    if (colHeader?.classList.contains('hide')) {
        const lastCell = tr.cells[tr.cells.length - 1];
        lastCell?.classList.add('hide');
    }
    return tr.cells[tr.cells.length - 1];
}
function columnSelectorCacheKey(table) {
    return `hiddenTableCols${window.location.pathname}${table.id}`;
}
function setHiddenColumns(table, state) {
    const cacheKey = columnSelectorCacheKey(table);
    window.sessionStorage.setItem(cacheKey, JSON.stringify(Array.from(state)));
}
function getHiddenColumns(table) {
    const cacheKey = columnSelectorCacheKey(table);
    return new Set(JSON.parse(window.sessionStorage.getItem(cacheKey) || '[]'));
}
function toggleCol(table, colIndex) {
    const allCol = table.querySelectorAll(`tr > *:nth-child(${colIndex + 1})`);
    for (let i = 0; i < allCol.length; i++) {
        allCol[i]?.classList.toggle('hide');
    }
}
function renderColumnSelector(table) {
    const container = table.parentElement;
    if (!container)
        return;
    container.insertAdjacentHTML('afterbegin', '<a class="col-toggle" id="col-toggle">+/-</a>');
    const hiddenColumns = getHiddenColumns(table);
    hiddenColumns.forEach((i) => {
        toggleCol(table, i);
    });
    function close() {
        const parent = container?.parentElement;
        parent?.querySelectorAll('.tooltip').forEach((el) => {
            parent.removeChild(el);
        });
    }
    container.addEventListener('click', (e) => {
        const target = e.target;
        if (!target.classList.contains('col-toggle'))
            return;
        const tooltip = container?.parentElement?.querySelector('.tooltip');
        if (tooltip) {
            close();
            return;
        }
        let str = '<form class="form tooltip"><a class="close">&times;</a>';
        const allCol = table.getElementsByTagName('th');
        for (let i = 0; i < allCol.length; i++) {
            const col = allCol[i];
            if (!col || col.innerHTML.length === 0) {
                continue;
            }
            const checked = !col.classList.contains('hide') ? 'checked' : '';
            str += `<label>
                  <span>${col.innerHTML}</span>
                  <input type="checkbox" class="col-toggle-checkbox" ${checked} data-index=${i}>
                </label>`;
        }
        str += '</form>';
        container.parentElement?.insertAdjacentHTML('beforeend', str);
        container.parentElement?.addEventListener('click', (ev) => {
            const tgt = ev.target;
            if (tgt.classList.contains('close'))
                close();
        });
        container.parentElement?.addEventListener('change', (ev) => {
            const tgt = ev.target;
            if (!tgt.classList.contains('col-toggle-checkbox'))
                return;
            const i = parseInt(tgt.dataset['index'] ?? '0', 10);
            toggleCol(table, i);
            const hidden = getHiddenColumns(table);
            if (hidden.has(i)) {
                hidden.delete(i);
            }
            else {
                hidden.add(i);
            }
            setHiddenColumns(table, hidden);
        });
        document.addEventListener('keyup', (ev) => {
            if (ev.key === 'Escape')
                close();
        });
    });
}
export function toggleDisplayError(tableID, message) {
    const tableError = document.getElementById(`${tableID}-error`);
    if (!tableError)
        return;
    if (message) {
        tableError.style.display = 'block';
        tableError.textContent = 'Something went wrong: ' + message;
    }
    else {
        tableError.style.display = 'none';
        tableError.textContent = '';
    }
}
//# sourceMappingURL=table.js.map