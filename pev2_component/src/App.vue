<template>
  <div id="app">
    <Pev2 :plan-source="plan" plan-query="" />
  </div>
</template>

<script>
import { Plan as Pev2 } from 'pev2';
import 'pev2/dist/style.css';
import planData from '../../plan.json'; // Adjust the path as necessary

export default {
    data() {
    let planString = JSON.stringify(planData, null, 2); // Convert the object to a JSON string with indentation for readability
        planString = `\`\n${planString}\n\``; // Enclose the string in backticks
    console.log(planString);
        return {
      plan: planString, // Assigning imported JSON data
      
    };
  },
  components: {
    Pev2,
  }
};
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>