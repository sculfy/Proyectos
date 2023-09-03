// CHART START
// 1. aquí hay que poner el código que genera la gráfica

let years;
let winners;
let originalData;

const width = 1000;
const height = 500;
const margin = {
    top: 50,
    bottom: 50,
    right: 30,
    left: 30
};

const svg = d3.select("div#chart")
    .append("svg")
    .attr("width", width)
    .attr("height", height);
const elementGroup = svg.append("g")
    .attr("class", "elementGroup")
    .attr("transform", `translate(${margin.left}, ${margin.top})`);
const axis = svg.append("g")
    .attr("class", "axis");
const xAxisGroup = axis.append("g")
    .attr("class", "xAxisGroup")
    .attr("transform", `translate(${margin.left}, ${height - margin.bottom})`);
const yAxisGroup = axis.append("g")
    .attr("class", "yAxisGroup")
    .attr("transform", `translate(${margin.left}, ${margin.top})`);

const x = d3.scaleBand().range([0, width - margin.left - margin.right]).padding(0.02);
const y = d3.scaleLinear().range([height - margin.bottom - margin.top, 0]);

const xAxis = d3.axisBottom().scale(x);
const yAxis = d3.axisLeft().scale(y);

// data:
    // 2. aquí hay que poner el código que requiere datos para generar la gráfica
    // update:
d3.csv("WorldCup.csv").then(data => {
    originalData = data;
    data.forEach(d => {
        d.Year = +d.Year;
    });

    years = data.map(d => d.Year);

    const wins_total = d3.nest().key(d => d.Winner).entries(data.filter(d => d.Winner !== ''));

    x.domain(data.filter(d => d.Winner !== '').map(d => d.Winner));
    y.domain([0, d3.max(wins_total.map(d => d.values.length))]);

    xAxisGroup.call(xAxis);
    yAxisGroup.call(yAxis);

    update(years[years.length-1]);
    slider();

});


// update:
    // 3. función que actualiza el gráfico
function update(year) {
    const graph_data = filterDataByYear(year);
    const max_win = d3.max(graph_data.map(d => d.values.length));

    x.domain(graph_data.map(d => d.key));
    y.domain([0, max_win]);
    yAxis.ticks(max_win);

    xAxisGroup.call(xAxis);
    yAxisGroup.call(yAxis);

    const columns = elementGroup.selectAll("rect").data(graph_data);

    columns.enter()
        .append("rect")
        .merge(columns)
        .attr("class", d => d.values.length < max_win ? 'rest_bar' : 'maximum_bar')
        .attr("x", d => x(d.key))
        .attr("y", d => y(d.values.length)) 
        .attr("width", x.bandwidth())
        .attr("height", d => height - margin.bottom - margin.top - y(d.values.length)); 

        columns.exit().remove();
}

// treat data:
    // 4. función que filtra los datos dependiendo del año que le pasemos (year)
function filterDataByYear(year) {
    const datafilter = originalData.filter(d => +d.Year <= year && d.Winner !== '');
    const winnerfilter = d3.nest().key(d => d.Winner).entries(datafilter);
    return winnerfilter;
}

// CHART END

// slider:
function slider() {    
    // esta función genera un slider:
    var sliderTime = d3
        .sliderBottom()
        .min(d3.min(years))  // rango años
        .max(d3.max(years))
        .step(4)  // cada cuánto aumenta el slider (4 años)
        .width(580)  // ancho de nuestro slider en px
        .ticks(years.length)  
        .default(years[years.length -1])  // punto inicio del marcador
        .on('onchange', val => {
            // 5. AQUÍ SÓLO HAY QUE CAMBIAR ESTO:
            // hay que filtrar los datos según el valor que marquemos en el slider y luego actualizar la gráfica con update
            d3.select('#value-time').text(val);
            update(val);
        });

        // contenedor del slider
        var gTime = d3 
            .select('div#slider-time')  // div donde lo insertamos
            .append('svg')
            .attr('width', width * 0.8)
            .attr('height', 100)
            .append('g')
            .attr('transform', 'translate(30,30)');

        gTime.call(sliderTime);  // invocamos el slider en el contenedor

        d3.select('p#value-time').text(sliderTime.value());  // actualiza el año que se representa
}