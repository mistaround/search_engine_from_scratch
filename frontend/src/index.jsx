import React from 'react';
import {createRoot} from 'react-dom/client';
import SearchPage from "./page/search";
import Result from './page/result';
import {
  BrowserRouter as Router,
  Route,
  Routes,
} from "react-router-dom";
import 'antd/dist/antd.min.css';
import "./App.css";

const rootElement = document.getElementById('root');
const root = createRoot(rootElement);

root.render(
  <Router>
    <Routes >
      <Route path="/" element={<SearchPage/>} />
      <Route path="/search" element={<Result/>} />
    </Routes >
  </Router>
);