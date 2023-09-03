const diCaprioBirthYear = 1974;
const age = function(year) { return year - diCaprioBirthYear}
const today = new Date().getFullYear()
const ageToday = age(today)

const width = 1000;
const height = 500;
const margin = {
    top: 50,
    right: 30,
    left: 30,
    bottom: 50
};

const svg = d3.select("div#chart").append("svg")
    .attr("width", width)
    .attr("height", height)
const elementGroup = svg.append("g")
    .attr("class", "elementGroup")
    .attr("class", "yAxisGroup")
    .attr("transform", `translate(${margin.left}, ${margin.top})`)
const axis = svg.append("g")
    .attr("class", "axis")
const xAxisGroup = axis.append("g")
    .attr("class", "xAxisGroup")
    .attr("transform", `translate(${margin.left}, ${height - margin.bottom})`)
const yAxisGroup = axis.append("g")
    .attr("class", "yAxisGroup")
    .attr("transform", `translate(${margin.left}, ${margin.top})`)

const x = d3.scaleLinear().range([0, width - margin.left - margin.right])
const y = d3.scaleLinear().range([height - margin.top - margin.bottom, 0])

const xAxis = d3.axisBottom().scale(x)
const yAxis = d3.axisLeft().scale(y)

d3.csv("data.csv").then(data => {
    data.forEach(d => {
        d.year = +d.year
        d.age = +d.age
});

    const diCaprio = data.map(d => ({ year: d.year, age: age(d.year) }))
    const ex_gf = data.map(d => ({ year: d.year, age: ageToday - age(d.year) }))

    x.domain([d3.min(data, d => d.year), d3.max(data, d => d.year)])
    y.domain([0, d3.max(diCaprio, d => d.age)]);

    xAxisGroup.call(xAxis);
    yAxisGroup.call(yAxis);

    const line = d3.line().x(d => x(d.year)).y(d => y(d.age))

    elementGroup.append("path")
        .datum(diCaprio)
        .attr("class", "line")
        .attr("d", line);

    const exs = x(data[1].year) - x(data[0].year)
        console.log("data[1].year:", data[1].year)
        console.log("data[0].year:", data[0].year)
        console.log("exs:", exs)
        console.log(exs)

    elementGroup.selectAll(".bar")
        .data(ex_gf)
        .enter().append("rect")
        .attr("class", "bar")
        .attr("x", d => x(d.year)) 
        .attr("y", d => y(d.age)) 
        .attr("width", exs)
        .attr("height", d => height - margin.top - margin.bottom - y(d.age));
});