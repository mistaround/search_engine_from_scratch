import React, { useState, useEffect } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { Row, Col, Divider, Input, List, Pagination, Spin } from 'antd';
import { SearchOutlined, LoadingOutlined } from '@ant-design/icons';
import logo from '../google.png';
import axios from "axios";

const { Search } = Input;

const PAGE_SIZE = 6;
// const BACKEND_URL = "http://3.91.219.72";
const BACKEND_URL = "http://localhost:45550";
const SUGGEST_URL = "http://localhost:8000";

function Result() {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);
    const [searchParams,] = useSearchParams();
    const [query, setQuery] = useState(searchParams.get("q"));
    const [input, setInput] = useState(query);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const [choice, setChoice] = useState([]);
    const [result, setResult] = useState([]);
    const [onFocusSearch, setOnFocusSearch] = useState(false);
    const [onFocusSelect, setOnFocusSelect] = useState(false);
    const [onSelect, setOnSelect] = useState(false);

    useEffect(() => {
        let start = Date.now()
        setInput(query);
        setLoading(true);
        axios.get(`${BACKEND_URL}/api/result?q=${query}&p=${page}`)
        .then((body) => {
            setLoading(false);
            setTotal(body.data.total);
            setResult(body.data.data);
            console.log(Date.now() - start);
        }).catch(() => {
            setLoading(false);
            setTotal(0);
            setResult([]);
        });
    }, [query, page]);

    useEffect(() => {
        if (input !== null && input.length !== 0) {
            if (input.charAt(input.length - 1) === ' ' && input.trim() !== '') {
                axios.get(`${SUGGEST_URL}/predict?q=${input}`)
                .then((message) => message.data.split("\n"))
                .then((data) => {
                    for (let i = 0; i < data.length; i++) {
                        data[i] = input + data[i];
                    }
                    setChoice(data);
                });
            }
        } else {
            setChoice([]);
        }
    }, [input])

    const handleChange = (e) => {
        setOnFocusSearch(true);
        setInput(e.target.value);
    };

    const handleClick = () => {
        setOnFocusSearch(false);
        setOnFocusSelect(false);
        navigate(`/search?q=${input}&p=1`);
        setQuery(input);
        setPage(1);
        setTotal(0);

    };

    const handleChoose = (obj) => {
        navigate(`/search?q=${obj}&p=1`);
        setQuery(obj);
        setPage(1);
        setTotal(0);
    }
    

    return (
        <div onMouseDown={() => {setOnFocusSelect(onSelect)}}>
            <Row style={{height: "2vh"}}/>
            <Row align="middle" style={{height: "4vh", marginLeft: 20}}>
                <p style={{fontSize: 14, color: "#cccccc"}}>
                    This is a course project of CIS555
                </p>
            </Row>
            <Row align="middle" style={{height: "10vh"}}>
                <Col span={4}/>
                <Col span={2}>
                    <img style={{height: "5vh"}} src={logo} alt="logo" onClick={() => navigate("/")} />  
                </Col>
                <Col span={8}>
                    <Search 
                        value={input}
                        onChange={(e) => handleChange(e)}
                        onSearch={() => handleClick()}
                        onFocus={() => setOnFocusSearch(true)}
                        onBlur={() => setOnFocusSearch(false)}
                        enterButton="Search" 
                        size="large" 
                        style={{
                            width: "39.5vw",
                            borderRadius: 24,
                        }}
                    />
                    {(input.length !== 0 && (onFocusSearch || onFocusSelect)) ? 
                    <div 
                        style={{ width: "33.7vw", overflow: "clip", backgroundColor: "white", position: "fixed", zIndex: 300 }}
                        onMouseEnter={() => {setOnSelect(true)}}
                        onMouseLeave={() => {setOnSelect(false)}}
                    >
                        <List 
                            bordered
                            itemLayout="horizontal"
                            dataSource={choice}
                            renderItem={item => (
                            <List.Item 
                                onClick={() => handleChoose(item)}>
                                <List.Item.Meta
                                style={{marginLeft: -10}}
                                avatar={<SearchOutlined />}
                                description={item}
                                />
                            </List.Item>
                        )}/>
                    </div>
                    : <></>}
                </Col>
                <Col span={10}/>
            </Row>
            <Divider/>
            <Row style={{minHeight: "100vmin"}}>
                <Col span={6}/>
                <Col span={9} >
                    {!loading ?
                    <><List
                            // style={{minHeight: "90vmin"}} 
                            itemLayout="vertical"
                            dataSource={result}
                            renderItem={item => {
                                const queryList = query.replaceAll(/[.,/#!$%^&*;:{}=\-_`~()]/g, " ").split(" ");
                                const url = (item.url.length > 60) ? String(item.url).slice(0, 60) + "..." : item.url;
                                const title = (item.title.length > 60) ? String(item.title).slice(0, 60) + "..." : item.title;
                                let description = (item.description.length > 256) ? String(item.description).slice(0, 256) + "..." : item.description;

                                for (let i = 0; i < queryList.length; i++) {
                                    var regEx = new RegExp("\\b" + queryList[i] + "\\b", "ig");
                                    description = description.replaceAll(regEx, "<b>" + queryList[i].toUpperCase() + "</b>");
                                }

                                return (
                                    <List.Item onClick={() => window.location.replace(item.url)}>
                                        <List.Item.Meta
                                            title={<a href={url}>{title}</a>}
                                            description={url} />
                                        <div dangerouslySetInnerHTML={{ __html: description }} />
                                        {/* {description} */}
                                    </List.Item>
                                );
                            } } />
                            {total !== 0 ? 
                            <Pagination style={{ marginTop: 20, alignItems: "center" }}
                            current={page}
                            onChange={(num) => {
                                navigate(`/search?q=${query}&p=${num}`);
                                setPage(num);
                            } }
                            pageSize={PAGE_SIZE}
                            total={total} />
                            :
                            <></>
                            }
                            
                            </> 
                            : 
                            <Row style={{height: "50vh"}} align="middle" justify="center">
                                <Spin tip="Loading" indicator={<LoadingOutlined spin />}/>
                            </Row>
                            }   
                </Col>
                <Col span={5}>
                    <Divider type="vertical" style={{marginTop: "1vh" , marginLeft: "1.8vw", minHeight: "80vmin"}}/>
                    {/* TODO: Sidebar */}
                </Col>
                <Col span={4}/>
            </Row>
            <Row style={{height: "10vh"}}/>
        </div>
    )
}

export default Result;