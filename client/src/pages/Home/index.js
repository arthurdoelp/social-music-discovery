import React, { Component } from 'react';
// import './style.css';
import axios from 'axios';
// import { Redirect } from 'react-router-dom';

class Home extends Component {
    constructor() {
        super()
        this.state = {
            errorAlert: "",
            message: ""
        }
    }


    render() {
        const { errorAlert, message } = this.state

        // Handles removing error text from alert onclick "x"
        const handleErrorAlert = e => {
            e.preventDefault()
            this.setState({ errorAlert: '' });
        }

        // Handle change from inputs
        // const handleChange = e => {
        //     this.setState({ [e.target.name]: e.target.value })
        // }

        const handleScrape = e => {
            e.preventDefault()
            axios.post('/api/scrape/new', {

            }).then(res => {
                console.log(res)
                this.setState({ message: res.data.data })
            }).catch(err => {
                console.log(err)
                this.setState({ errorAlert: "There is an error" })
            })
        }

        return (
            <div>
                <div className="container">

                    {/* This is for any alerts/errors */}
                    {(errorAlert) ?
                        <div className="alert alert-danger alert-dismissible fade show" role="alert">
                            {errorAlert}
                            <button type="button" className="close" onClick={handleErrorAlert} data-dismiss="alert" aria-label="Close">
                                <span aria-hidden="true">&times;</span>
                            </button>
                        </div>
                        : null}

                    <div className="row">
                        <div className="col-lg-3 col-md-1"></div>
                        <div className="col-lg-6 col-md-10">
                            {/* Page Title */}
                            <div className="row">
                                <div className="col text-center mt-5">
                                    <h2>Scrape Data</h2>
                                    <button onClick={handleScrape}><h1>Scrape</h1></button>
                                    <h1>{message}</h1>
                                </div>
                            </div>

                        </div>
                        <div className="col-lg-3 col-md-1"></div>
                    </div>
                </div>
            </div>
        )
    }
}

export default Home;