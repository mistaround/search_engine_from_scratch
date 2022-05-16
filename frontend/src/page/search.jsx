import React, {useState, useEffect} from "react";
import { useNavigate } from "react-router-dom";
import logo from '../google.png';
import axios from "axios";
import { Row, Col, Divider, Input, List } from 'antd';
import { SearchOutlined } from '@ant-design/icons';

const { Search } = Input;
const SUGGEST_URL = "http://localhost:8000";

export default function SearchPage() {
    const navigate = useNavigate();

    const [choice, setChoice] = useState([]);
    const [input, setInput] = useState("");

    useEffect(() => {
        if (input.length !== 0) {
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
        setInput(e.target.value);
    };

    const handleClick = () => {
        navigate(`/search?q=${input}&p=1`);
    };

    const handleChoose = (obj) => {
        navigate(`/search?q=${obj}&p=1`);
    }

    return (
        <>
            <Row style={{height: "2vh"}}/>
            <Row style={{height: "8vh", marginLeft: 20 }}>
                <p style={{fontSize: 14, color: "#cccccc"}}>
                    This is a course project of CIS555
                </p>
            </Row>
            <Row justify="center" align="middle">
                <Col>
                    <Row style={{height: "5vh"}}/>
                    <Row justify="center" align="middle" style={{height: "25vh"}}>
                        <img style={{height: "6vw"}} src={logo} alt="logo" />
                    </Row>
                    <Row justify="center" align="top" style={{height: "10vh" }}>
                        <Search 
                            onChange={(e) => handleChange(e)}
                            onSearch={() => handleClick()}
                            value={input}
                            placeholder="input search keyword with space to see suggestions" 
                            enterButton="Search" 
                            size="large" 
                            style={{
                                width: "40vw",
                                borderRadius: 24,
                            }}
                        />
                    </Row>
                    <Row justify="center" align="top" style={{height: "35vh", borderWidth: 1, width: "34vw", marginTop: "-4vh"}}>
                        {input.length !== 0 ? 
                        <List 
                            style={{width: "34vw"}}
                            itemLayout="horizontal"
                            dataSource={choice}
                            renderItem={item => {
                                return (
                                <List.Item style={{overflow: "clip"}} onClick={() => handleChoose(item)}>
                                    <List.Item.Meta
                                    style={{marginLeft: 15, marginRight: 15}}
                                    avatar={<SearchOutlined />}
                                    description={item}
                                    />
                                </List.Item>
                                );
                            }}/> : <></>}
                    </Row>
                    <Row style={{height:"5vh"}}/>
                    <Row>
                        <Divider/>
                    </Row>
                    <Row justify="center" align="middle" style={{fontSize: 14, color: "#555555"}}>
                        * CIS555 PROJECT SEARCH ENGINE *
                    </Row>
                    <Row style={{height:"5vh"}}/>
                </Col>
            </Row>
        </>
    )
}
