/**
 * Javascript for creating charts for Operator/Country reports
 *
 * Copyright (c) 2018-2021 Qualcomm Technologies, Inc.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
 * limitations in the disclaimer below) provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or
 * promote products derived from this software without specific prior written permission.
 * - The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
 * If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
 * details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
 * - Altered source versions must be plainly marked as such, and must not be misrepresented as being the original
 * software.
 * - This notice may not be removed or altered from any source distribution.
 *
 * NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED
 * BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

(function(chart_data, d3, window, document) {

  'use strict';

  // Use the same chart width in all charts
  var chartWidth = 630;

  function drawIDTrendsChart(historic_imei_counts,
                             historic_imsi_counts,
                             historic_msisdn_counts,
                             dates) {

    if (!historic_imei_counts || !historic_imsi_counts || !historic_msisdn_counts) {
      throw 'Data is NULL or undefined!';
    }

    if (!dates) {
      throw 'No month dates supplied';
    }

    var container = document.querySelector('#id_trends_chart');
    if (!container) {
      throw 'ID trends chart container could not be found!';
    }

    var chartElement = d3.select('#id_trends_chart .d3-chart');
    if (!chartElement) {
      throw 'ID trends D3 chart container element is NULL or undefined!';
    }

    // Work out required height in pixels
    var chartHeight =  400;
    var margin = { top: 10, right: 30, bottom: 50, left: 70 };
    var width = chartWidth - margin.left - margin.right;
    var height = chartHeight - margin.top - margin.bottom;

    var rootElem = chartElement.append('svg')
                    .attr('width', chartWidth)
                    .attr('height', chartHeight)
                    .append('g')
                    .attr('transform',  'translate(' + margin.left + ',' + margin.top + ')');

    var yMax = Math.max.apply(null, historic_imei_counts
                                    .concat(historic_imsi_counts)
                                    .concat(historic_msisdn_counts));

    var yScale = d3.scaleLinear()
                   .domain([1.05 * yMax, 0])
                   .nice()
                   .range([0, height]);
    var xScale = d3.scaleTime()
                   .domain(d3.extent(dates))
                   .rangeRound([0, width]);

    var line = d3.line()
      .x(function(_, idx) {
        return xScale(dates[idx]);
      })
      .y(function(value) {
        return yScale(value);
      });

    // Insert x-axis
    rootElem.append('g')
      .attr('transform', 'translate(0,' + height + ')')
      .call(d3.axisBottom(xScale).ticks(6).tickFormat(d3.timeFormat('%b-%Y')));

    // Insert y-axis
    rootElem.append('g').call(d3.axisLeft(yScale));

    // Insert IMEI counts
    rootElem.append('svg:path')
      .classed('trendline', true)
      .classed('imei-trend', true)
      .attr('d', line(historic_imei_counts))
      .attr('fill', 'none');

    rootElem.selectAll('dot')
      .data(historic_imei_counts)
      .enter()
      .append('circle')
        .classed('imei-circle', true)
        .attr('r', 5)
        .attr('cx', function(d, idx) { return xScale(dates[idx]); })
        .attr('cy', function(d) { return yScale(d); });

    // Insert MSISDN counts
    rootElem.append('svg:path')
      .classed('trendline', true)
      .classed('msisdn-trend', true)
      .attr('d', line(historic_msisdn_counts))
      .attr('fill', 'none');

    rootElem.selectAll('dot')
      .data(historic_msisdn_counts)
      .enter()
      .append('circle')
        .classed('msisdn-circle', true)
        .attr('r', 5)
        .attr('cx', function(d, idx) { return xScale(dates[idx]); })
        .attr('cy', function(d) { return yScale(d); });

    // Insert IMSI counts
    rootElem.append('svg:path')
      .classed('trendline', true)
      .classed('imsi-trend', true)
      .attr('d', line(historic_imsi_counts))
      .attr('fill', 'none');

    rootElem.selectAll('dot')
      .data(historic_imsi_counts)
      .enter()
      .append('circle')
        .classed('imsi-circle', true)
        .attr('r', 5)
        .attr('cx', function(d, idx) { return xScale(dates[idx]); })
        .attr('cy', function(d) { return yScale(d); });

    // Make container visible
    container.style.display = 'block';
  }

  function drawComplianceBreakdownChart(data) {
    if (!data) {
      throw 'Compliance data is NULL or undefined!';
    }

    var container = document.querySelector('#compliance_breakdown_chart');
    if (!container) {
      throw 'Compliance chart container could not be found!';
    }

    var chartElement = d3.select('#compliance_breakdown_chart .d3-chart');
    if (!chartElement) {
      throw 'Compliance D3 chart container element is NULL or undefined!';
    }

    var noncompliant = data.meets_blocking;
    var compliant = data.meets_none + data.meets_non_blocking;
    var total = compliant + noncompliant;

    if (total > 0) {
      var d3Data = [{
        isCompliant: true,
        value: compliant
      }, {
        isCompliant: false,
        value: noncompliant
      }];

      // Work out required height in pixels
      var width = 600;
      var height =  350;
      var radius = Math.min(width, height) / 2;

      var svg = chartElement.append('svg')
          .attr('width', width)
          .attr('height', height)
          .append('g')
          .attr('transform', 'translate(' + width / 2 + ',' + height / 2 + ')');

      var smallArc = d3.arc()
                      .outerRadius(radius-10)
                      .innerRadius(0);

      var largeArc = d3.arc()
                      .outerRadius(radius)
                      .innerRadius(0);

      var pie = d3.pie()
        .sort(null)
        .value(function(d) { return d.value; });

      svg.selectAll('path')
        .data(pie(d3Data))
        .enter()
        .append('path')
        .each(function(d) {
          var e = d3.select(this);
          if (d.data.isCompliant) {
            e.attr('d', smallArc);
          } else {
            e.attr('d', largeArc);
          }
          e.classed(d.data.isCompliant ? 'segment-compliant' : 'segment-noncompliant', true);
        });

      container.style.display = 'block';
    }
  }

  function drawComplianceTrendsChart(data, dates) {
    if (!data) {
      throw 'Data is NULL or undefined!';
    }

    if (!dates) {
      throw 'No month labels supplied';
    }

    var container = document.querySelector('#compliance_trends_chart');
    if (!container) {
      throw 'Compliance trends chart container could not be found!';
    }

    var chartElement = d3.select('#compliance_trends_chart .d3-chart');
    if (!chartElement) {
      throw 'Compliance trends D3 chart container element is NULL or undefined!';
    }

    // Work out required height in pixels
    var chartHeight =  400;
    var margin = { top: 10, right: 30, bottom: 50, left: 70 };
    var width = chartWidth - margin.left - margin.right;
    var height = chartHeight - margin.top - margin.bottom;

    var rootElem = chartElement.append('svg')
                    .attr('width', chartWidth)
                    .attr('height', chartHeight)
                    .append('g')
                    .attr('transform',  'translate(' + margin.left + ',' + margin.top + ')');

    var values = data.map(function(d) {
      var total = d.num_compliant_imeis + d.num_noncompliant_imeis_blocking + d.num_noncompliant_imeis_info_only;
      if (total == 0) {
        return 0;
      }

      return 100.0 * d.num_noncompliant_imeis_blocking / total;
    });

    var yMax = Math.max.apply(null, values);

    var yScale = d3.scaleLinear()
                   .domain([yMax * 1.05, 0])
                   .nice()
                   .range([0, height]);
    var xScale = d3.scaleTime()
                   .domain(d3.extent(dates))
                   .rangeRound([0, width]);

    var line = d3.line()
      .x(function(_, idx) {
        return xScale(dates[idx]);
      })
      .y(function(value) {
        return yScale(value);
      });

    // Insert x-axis
    rootElem.append('g')
      .attr('transform', 'translate(0,' + height + ')')
      .call(d3.axisBottom(xScale).ticks(6).tickFormat(d3.timeFormat('%b-%Y')));

    // Insert y-axis
    rootElem.append('g').call(d3.axisLeft(yScale));

    // Insert IMEI counts
    rootElem.append('svg:path')
      .classed('trendline', true)
      .classed('imei-trend', true)
      .attr('d', line(values))
      .attr('fill', 'none');

    rootElem.selectAll('dot')
      .data(values)
      .enter()
      .append('circle')
        .classed('imei-circle', true)
        .attr('r', 5)
        .attr('cx', function(d, idx) { return xScale(dates[idx]); })
        .attr('cy', function(d) { return yScale(d); });

    // now add titles to the axes
    rootElem
        .append('text')
        .attr('text-anchor', 'middle')
        .attr('transform', 'translate(' + (20-margin.left) + ' ' +(height/2)+') rotate(-90)')
        .text('% of observed IMEIS which are non-compliant');

    // Make container visible
    container.style.display = 'block';
  }

  function drawConditionBreakdownChart(sorted_conditions, data_map) {
    if (!sorted_conditions) {
      throw 'Sorted conditions are NULL or undefined!';
    }

    if (!data_map) {
      throw 'Condition data is NULL or undefined!';
    }

    var data = [];
    sorted_conditions.forEach(function(condition) {
      var data_val = data_map[condition.label];
      data_val.blocking = condition.blocking;
      data.push(data_val);
    });

    if (data.length > 0) {
      var container = document.querySelector('#condition_breakdown_chart');
      if (!container) {
        throw 'Condition breakdown chart container could not be found!';
      }

      var chartElement = d3.select('#condition_breakdown_chart .d3-chart');
      if (!chartElement) {
        throw 'Condition breakdown D3 chart container element is NULL or undefined!';
      }

      // Work out required height in pixels
      var chartHeight =  data.length * 70;
      var margin = { top: 10, right: 30, bottom: 40, left: 40 };
      var width = chartWidth - margin.left - margin.right;
      var height = chartHeight - margin.top - margin.bottom;

      var rootElem = chartElement.append('svg')
                      .attr('width', chartWidth)
                      .attr('height', chartHeight)
                      .append('g')
                      .attr('transform',  'translate(' + margin.left + ',' + margin.top + ')');

      data.forEach(function(condition, index) {
        condition.label = 'C' + (index + 1);
      });

      var labels = data.map(function (condition) { return condition.label; });
      var xMax = d3.max(data, function (condition) { return condition.num_imeis; });

      // add padding so bars don't reach edge
      var xScale = d3.scaleLinear()
                     .domain([0, 1.15 * xMax])
                     .nice()
                     .range([0, width]);
      var yScale = d3.scaleBand()
                     .domain(labels)
                     .rangeRound([0, height])
                     .paddingInner(0.3)
                     .paddingOuter(0.6);

      var bar = rootElem.selectAll('g')
        .data(data)
        .enter()
        .append('g')
        .attr('transform', function(c) { return 'translate(0,' + yScale(c.label) + ')'; });

      bar.append('rect')
        .classed('barchart-bar', true)
        .attr('width', function(c) { return xScale(c.num_imeis); })
        .attr('height', yScale.bandwidth())
        .classed('blocking', function(c) {
          return c.blocking;
        });

      bar.append('text')
        .attr('y', yScale.bandwidth() / 2)
        .attr('x', function(c) { return xScale(c.num_imeis) + 10; })
        .attr('dy', '.4em')
        .text(function(c) { return c.num_imeis.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ','); });

      // insert x-axis
      rootElem.append('g')
        .classed('xaxis-container', true)
        .attr('transform', 'translate(0,' + height + ')')
        .append('path')
        .attr('d', 'M 0.5 0.5 H ' + (width-1) + ' 0')
        .attr('stroke', '#000');

      // insert y-axis
      rootElem.append('g').call(d3.axisLeft(yScale).tickSize(0).tickPadding(15));

      // Add x-axis title
      rootElem
        .append('text')
        .attr('text-anchor', 'middle')
        .attr('transform', 'translate(' + (width/2) + ' ' + (height+margin.top+20) +')')
        .text('IMEI Count Meeting Condition');

      container.style.display = 'block';
    }
  }

  function drawConditionSparklines(sorted_conditions, data_map) {
    if (!sorted_conditions) {
      throw 'Sorted conditions are NULL or undefined!';
    }

    if (!data_map) {
      throw 'Condition data is NULL or undefined!';
    }

    // create an SVG element inside the #graph div that fills 100% of the div
    var elems = Array.prototype.slice.call(document.querySelectorAll('.tg-trend'));
    elems.forEach(function(elem, idx) {
      var sparklineWidth = 120;
      var sparklineHeight = 46;
      var rootElem = d3.select(elem);
      var graph = rootElem.append('svg:svg')
                    .attr('width', sparklineWidth)
                    .attr('height', sparklineHeight);
      var data = data_map[sorted_conditions[idx].label].map(function(e) { return e.num_imeis; });
      var maxVal = Math.max.apply(null, data);
      var xScale = d3.scaleLinear().domain([0, 5]).range([0, sparklineWidth]);
      var yScale = d3.scaleLinear().domain([0, maxVal]).range([sparklineHeight - 10, 10]);

      var area = d3.area()
        .x(function(_, idx) {
          return xScale(idx);
        })
        .y0(function() {
          return yScale(0);
        })
        .y1(function(value) {
          return yScale(value);
        });

      var line = d3.line()
        .x(function(_, idx) {
          return xScale(idx);
        })
        .y(function(value) {
          return yScale(value);
        });

      var axis = d3.line()
        .x(function(_, idx) {
          return xScale(idx);
        })
        .y(function() {
          return yScale(0);
        });

      graph.append('svg:path')
        .attr('d', area(data))
        .attr('fill', 'rgb(223,237,250)')
        .attr('stroke-width', 0);

      graph.append('svg:path')
        .attr('d', axis(data))
        .attr('fill', 'none')
        .attr('stroke', 'rgb(208,221,232)');

      graph.append('svg:path')
        .attr('d', line(data))
        .attr('fill', 'none')
        .attr('stroke', 'rgb(160,202,239)');
    });
  }

  /*
   * Parse the JSON data. On success, calls each graph function
   * in turn with the JSON data
   */
  function drawAllCharts() {

    var data = JSON.parse(chart_data);

    // If we don't have any data for the report, don't try to draw any graphs
    if (!data.has_data) {
      return;
    }

    // Work out the date labels for trends
    var d = new Date(data.end_date);
    var month = d.getMonth();
    var year = d.getFullYear();
    var dates = [];
    for(var i = 0; i < 6; i++) {
      dates.push(new Date(year, month));
      month -= 1;
      if (month < 0) {
        month = 11;
        year -= 1;
      }
    }
    dates.reverse();

    // Draw ID trends chart
    try {
      drawIDTrendsChart(data.historic_imei_counts,
                        data.historic_imsi_counts,
                        data.historic_msisdn_counts,
                        dates);
    } catch (e) {
      window.alert('Error during drawing of ID trends data - charts could not be drawn!');
      console.error(e);
    }

    // Draw compliance breakdown chart
    try {
      drawComplianceBreakdownChart(data.compliance_breakdown);
    } catch (e) {
      window.alert('Error during drawing of compliance breakdown data - charts could not be drawn!');
      console.error(e);
    }

    // Draw compliance trends chart
    try {
      drawComplianceTrendsChart(data.historic_compliance_breakdown, dates);
    } catch (e) {
      window.alert('Error during drawing of historic compliance data - chart could not be drawn!');
      console.error(e);
    }

    // Draw condition breakdown chart
    try {
      drawConditionBreakdownChart(data.classification_conditions, data.conditions_breakdown);
    } catch (e) {
      window.alert('Error during drawing of compliance breakdown data - charts could not be drawn!');
      console.error(e);
    }

    // Draw condition sparklines
    try {
      drawConditionSparklines(data.classification_conditions, data.historic_conditions_breakdown);
    } catch (e) {
      window.alert('Error during drawing of condition sparklines - sparklines could not be drawn!');
      console.error(e);
    }
  }

  // When script has finished loading, kick off the chart drawing process
  drawAllCharts();

}(window.DIRBS_CHART_DATA, window.d3, window, document));
