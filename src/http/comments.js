// SPDX-License-Identifier: MIT OR Apache-2.0
// Author: Kan-Ru Chen
(function (global) {
    const default_avatar = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACgAAAAoCAYAAACM/rhtAAAABGdBTUEAALGPC/xhBQAAAAFzUkdCAdnJLH8AAAAgY0hSTQAAeiYAAICEAAD6AAAAgOgAAHUwAADqYAAAOpgAABdwnLpRPAAAAAZiS0dEAAAAAAAA+UO7fwAAAAlwSFlzAAAuIwAALiMBeKU/dgAAAAd0SU1FB+kCFwQBL/9SeGgAAARVSURBVFjD7VhdaBxVFP6+O7tNmkTQKhXrD0YLa2JnN8nsbAORxgffNCpCkQrSB6HgD1hNoBYDImhLtSiK6JtFpSpaRa21rxJaMDs7u5tduyhII7RRQ60/LcGY7NzjQzbtZjLJJpM2jZDzeO65936cc+4537nAqixNGKQ0OzZ3kGJryNX+NSU8Xsimj4W5LN6RuktTumaj4J/UzBSzg1n/UmQGsGSyh+A+QFoAQAXgF8iLAEIBFMo9inwhcFEJTMsuUWNXIed8fUF9EVxqL6G+AthyxcJJtsLgYdOy98wAGLfsZwk8t8CcOBsagOCPBQLdbSZTOwGALe3tt0RV5EeQ9bVjJKOGLrfncrlfwwBMJDo36KiXI7i+9lXyjwcdi0RV5MkgcAL5lgIHpK4ohjH57+e5QuFMWA8ODX33Szwe34Ro/UMgmitADBBJgnf7vLjWgPEETStVJLHJB/+VguvsWs78My37VZJ9PjcVFCm3+o3LCq8t9wPxFPYHlIxmBbDJry85zuhyAwy8k7xKrfROEgl4PeVam1pbWxuN+sYHAMAbH/uyVCqNLcWu+m6SMzAFeJC5+Q6JxbqaIg2NDhUPUvFgpKHRicW6msLa+WpsflaDqULvQTDgedg+3yF1TRMPz+w2bJnShbObAVBhOwQDIuJdCHEhk+Yi0yJ6iXUX66TjlAB01wjx/DIO/SlETlclzukJhU/C2oWiW7Wkra3turIRfRQAIt7kB/l8/vel2K3KqvzvZpLAecKyUppGW51477quWw5z2U2dnfXXlPVjCjo9lMk4oVrd3GJ8o4BrJ2jEAPSGAbhu0tsLcqeGOgNg/UL2LKYO5iouf8a0UtsWzfc67EdAPj11huQXTBbiyZRMtzqCx6nk8UpF901kegeFGZDrSByIWykpuOmPF0ZGU9tIHJjChrMwuCNwJLDtVhG8L4LENGlQVRTbALFFNN4L2lzMZIYBfT9EzgOoA/GRadmH7mxPxedkPbadMK3UZyQ+BLBGRM5p6p5COv1zIJvRfAegVc1oOO3BaspTdJ3o3KHa3AHqQySbq3b9BOGgEKOV6e16EJ0Abq9ixydFuDVoOK+i/ZN+uhUJGPnmfTjF7GDWNM0OqVvbT5HKwMWNIDYyqDaIjAvwluFNvpTP5/+uMW5GlvCKq0AWi38B6LvDsvavEfWgEPcRuA3AzRWTUwKcpPBwWeGLpYwQjCft8/65ZEK8G35w3d+WsyAnEp0bJKpHfOl2TgEYnk3aVO9ydwwd1b0BXWQ4AsFREKYvF/riVsrWxABExi7r7xbZqATdALbMLm08GhGDb0LjKQINPvjdCugGefl/tzjn18fbqphOj4hI/8pjCdxdct1TCgC+d53XRWQPAFkB0AQiLxcz6TdmdJKi6zxPLT0iOHHFkAlOaPHuLbhOf2AdHMo6RwAcSSSTbRqwIbyRvp8vgT4W3jX6GGHs8+XaOCgjCnAKbiaPVbnE8h/vS9p4vH6MEwAAAABJRU5ErkJggg==';
    var styleCallback = function () {
        const style = document.createElement('style');
        style.textContent = `
            .pinka-comments .pinka-comment {
                border: 1px solid #ccc;
                border-radius: 5px;
                padding: 10px;
                margin: 10px 0;
                display: flex;
                flex-direction: column;
            }
            .pinka-comments .pinka-comment-author {
                display: flex;
                align-items: center;
            }
            .pinka-comments .pinka-comment-author-avatar {
                width: 40px;
                height: 40px;
                border-radius: 5px;
                margin-right: 10px;
            }
            .pinka-comments .pinka-comment-author-name-container {
                display: flex;
                flex-direction: column;
            }
            .pinka-comments .pinka-comment-author-name {
                font-weight: bold;
                text-decoration: none;
                color: black;
            }
            .pinka-comments .pinka-comment-content {
                margin-left: 50px;
            }
            .pinka-comments .pinka-load-more {
                display: block;
                margin: 10px auto;
                padding: 10px 20px;
                background-color: #007bff;
                color: #fff;
                border: none;
                cursor: pointer;
            }
            @media (prefers-color-scheme: dark) {
                .pinka-comments .pinka-comment {
                    border-color: #444;
                }
                .pinka-comments .pinka-comment-author-avatar.default-avatar {
                    filter: invert(1);
                }
                .pinka-comments .pinka-comment-author-name {
                    color: #eee;
                }
                .pinka-comments .pinka-load-more {
                    background-color: #0056b3;
                }
            }
        `;
        document.head.appendChild(style);
    };

    const enrichAuthorDetails = async function (items) {
        const options = {
            headers: {
                'Accept': 'application/ld+json; profile="https://www.w3.org/ns/activitystreams"'
            },
            cache: 'no-cache'
        };
        const authorIds = items.map(item => item.attributedTo);
        const uniqueAuthorIds = new Set(authorIds);
        const authorDetailsPromises = Array.from(uniqueAuthorIds).map(async (authorId) => {
            const authorResponse = await fetch(authorId, options);
            return authorResponse.json();
        });

        const authorDetails = await Promise.all(authorDetailsPromises);

        const authorDetailsMap = new Map();
        authorDetails.forEach(author => {
            authorDetailsMap.set(author.id, author);
        });

        items.forEach(item => {
            item.authorDetails = authorDetailsMap.get(item.attributedTo);
        });

        return items;
    };

    const renderOnePage = function (items) {
        const container = document.querySelector('.pinka-comments .pinka-comments-list');
        if (!container) return;

        // Render the replies
        items.forEach(function (reply) {
            const commentDiv = document.createElement('div');
            commentDiv.className = 'pinka-comment';

            const authorDiv = document.createElement('div');
            authorDiv.className = 'pinka-comment-author';

            const avatarImg = document.createElement('img');
            avatarImg.className = 'pinka-comment-author-avatar';
            avatarSrc = reply.authorDetails && reply.authorDetails.icon ? reply.authorDetails.icon.url : '';
            if (!avatarSrc) {
                avatarSrc = default_avatar;
                avatarImg.classList.add('default-avatar');
            }
            avatarImg.src = avatarSrc;
            avatarImg.alt = reply.authorDetails ? reply.authorDetails.name : 'Unknown';

            const authorNameContainer = document.createElement('div');
            authorNameContainer.className = 'pinka-comment-author-name-container';

            const authorNameSpan = document.createElement('a');
            authorNameSpan.className = 'pinka-comment-author-name';
            authorNameSpan.textContent = reply.authorDetails ? reply.authorDetails.name : 'Unknown';
            authorNameSpan.href = reply.authorDetails ? reply.authorDetails.id : '#';
            authorNameSpan.target = '_blank';

            const authorPreferredNameSpan = document.createElement('span');
            authorPreferredNameSpan.className = 'pinka-comment-author-preferred-name';
            authorPreferredNameSpan.textContent = reply.authorDetails && reply.authorDetails.preferredUsername ? `@${reply.authorDetails.preferredUsername}` : '';

            authorNameContainer.appendChild(authorNameSpan);
            authorNameContainer.appendChild(authorPreferredNameSpan);

            authorDiv.appendChild(avatarImg);
            authorDiv.appendChild(authorNameContainer);

            const contentDiv = document.createElement('div');
            contentDiv.className = 'pinka-comment-content';
            // SAFETY: Pinka is a trusted source, so we can safely set the innerHTML
            contentDiv.innerHTML = reply.content;

            commentDiv.appendChild(authorDiv);
            commentDiv.appendChild(contentDiv);

            container.appendChild(commentDiv);
        });
    };

    var renderCallback = async function (repliesPage) {
        const container = document.querySelector('.pinka-comments');
        if (!container) return;

        const comments_list = document.createElement('div');
        comments_list.className = 'pinka-comments-list';
        container.appendChild(comments_list);

        const enrichedItems = await enrichAuthorDetails(repliesPage.items || repliesPage.orderedItems);
        renderOnePage(enrichedItems);

        // Add a load more button
        const loadMoreButton = document.createElement('button');
        loadMoreButton.className = 'pinka-load-more';
        loadMoreButton.textContent = 'Load More';

        loadMoreButton.onclick = async function () {
            if (repliesPage.next) {
                const options = {
                    headers: {
                        'Accept': 'application/ld+json; profile="https://www.w3.org/ns/activitystreams"'
                    },
                    cache: 'no-cache'
                };
                const nextPageResponse = await fetch(repliesPage.next, options);
                const nextPage = await nextPageResponse.json();
                if (nextPage && nextPage.orderedItems) {
                    const enrichedNextPageItems = await enrichAuthorDetails(nextPage.orderedItems);
                    renderOnePage(enrichedNextPageItems);
                    repliesPage = nextPage; // Update the current page to the next page
                }
            }
        };

        container.appendChild(loadMoreButton);
    };

    global.Pinka = {
        setStyleFn: function (callback) {
            styleCallback = callback;
        },
        setRenderFn: function (callback) {
            renderCallback = callback;
        },
        href: window.location.href
    };

    document.addEventListener('DOMContentLoaded', function () {
        styleCallback();
        // Fetch the replies and call renderCallback with the replies
        fetchReplies().then(renderCallback);
    });

    async function fetchReplies() {
        const headers = {
            'Accept': 'application/ld+json; profile="https://www.w3.org/ns/activitystreams"'
        };
        const response = await fetch(global.Pinka.href, {
            headers
        });
        const data = await response.json();
        if (!data || !data.replies || !data.replies.id || !data.replies.totalItems || data.replies.totalItems === 0) {
            console.debug('No replies found');
            return { items: [] };
        }
        const repliesResponse = await fetch(data.replies.id, {
            headers
        });
        const replies = await repliesResponse.json();
        if (!replies || !replies.first) {
            console.debug('No replies found');
            return { items: [] };
        }
        const firstPageResponse = await fetch(replies.first, {
            headers
        });
        const firstPage = await firstPageResponse.json();
        if (!firstPage || !firstPage.orderedItems) {
            console.debug('No replies found');
            return { items: [] };
        }
        return firstPage;
    }
})(window);
