document.addEventListener('contextmenu', function (e) {
  // إلغاء الحدث الافتراضي لنقرة الزر الأيمن
  e.preventDefault();
});
// تعطيل عرض أدوات المطور
document.onkeydown = function(e) {
if (e.keyCode == 123) { // F12
  return false;
}
if (e.ctrlKey && e.shiftKey && e.keyCode == 'I'.charCodeAt(0)) { // Ctrl + Shift + I
  return false;
}
if (e.ctrlKey && e.shiftKey && e.keyCode == 'J'.charCodeAt(0)) { // Ctrl + Shift + J
  return false;
}
if (e.ctrlKey && e.keyCode == 'U'.charCodeAt(0)) { // Ctrl + U
  return false;
}
}
loader =
  '<div class="loader"><div class="lds-ellipsis"><div></div><div></div><div></div><div></div></div></div>';
function CloseTrailer() {
  $(".PopupTrailer").removeClass("Opened").find(".content").html("");
}
$("body").on("click", ".ShowTrailerSingle", function (event) {
  event.stopPropagation();
  $(".PopupTrailer").addClass("Opened").find(".content").html("");
  href = $(this).data("url");
  $.ajax({
    url: MyAjaxURL + "Home/LoadTrailer.php",
    type: "POST",
    data: { href: href },
  }).done(function (date) {
    $(".PopupTrailer").find(".content").html(date);
  });
});
$(document).mouseup(function (e) {
  var container = $(".filterPosts #filterForm > ul > li > ul > li");
  if (!container.is(e.target) && container.has(e.target).length === 0) {
    $(".filterPosts.seriesFilter #filterForm > ul >li").removeClass("active");
  }
});
var ImagesLoading = function () {
  setTimeout(function () {
    $("[data-src]").each(function (index) {
      let that = $(this);
      if (
        $(window).scrollTop() + $(window).height() >
        $(this).offset().top + 100
      ) {
        $(this).attr("src", $(this).data("src"));
        $(this).removeAttr("data-src");
      }
    });
  }, 300);
};
$(window).on("scroll", ImagesLoading);
$(window).on("resize", ImagesLoading);
$(window).on("load", ImagesLoading);
$(window).ajaxSuccess(ImagesLoading);
let lastScrollTop = 0;
let navbarHeight = $(".Main--Header").outerHeight();
let MathNUm = 15;
function hasScrolled() {
  let st = $(this).scrollTop();
  if (Math.abs(lastScrollTop - st) <= MathNUm) return;
  if (st > lastScrollTop && st > navbarHeight) {
    $(".Main--Header").addClass("Header--Onscroll");
  } else {
    if (st + $(window).height() < $(document).height()) {
      $(".Main--Header").removeClass("Header--Onscroll");
    }
  }
  lastScrollTop = st;
}
jQuery(document).ready(function ($) {
  $(".openChat").click(function () {
    $(".chatBox").addClass("active");
  });
  $(".chatBox .close").click(function () {
    $(".chatBox").removeClass("active");
  });
  $(".Slides--Main").owlCarousel({
    stopOnHover: !0,
    smartSpeed: 400,
    nav: !0,
    mouseDrag: !1,
    slideBy: 1,
    rtl: !0,
    items: 1,
    responsiveClass: !0,
    margin: 15,
    loop: !0,
    addClassActive: !0,
    autoplay: !0,
    autoplayTimeout: 4000,
    autoplayHoverPause: !0,
    responsive: {
      0: { items: 1 },
      400: { items: 2.5 },
      600: { items: 2.5 },
      1000: { items: 3 },
      1200: { items: 5 },
    },
  });
  setTimeout(function () {
    $(".Slider--Outer").css({ height: "auto" });
  }, 500);
  $(".Arrow--Right").click(function () {
    $(".Slides--Main").find(".owl-next").click();
  });
  $(".Arrow--Left").click(function () {
    $(".Slides--Main").find(".owl-prev").click();
  });
  $(".SlidesList").owlCarousel({
    autoplay: 10000,
    nav: !1,
    dots: !0,
    loop: !0,
    items: 6,
    autoWidth: !0,
  });
  $(".HomeSlider span.next").click(function () {
    $(".SlidesList .owl-next").click();
  });
  $(".HomeSlider span.prev").click(function () {
    $(".SlidesList .owl-prev").click();
  });
  $("body").css("padding-top", navbarHeight);
  doScrolled = !1;
  $(window).on("scroll", function () {
    doScrolled = !0;
    if ($(this).scrollTop()) {
      $(".Main--Header").addClass("Fixed--Head");
    } else {
      $(".Main--Header").removeClass("Fixed--Head");
    }
  });
  setInterval(function () {
    if (doScrolled) {
      hasScrolled();
      doScrolled = !1;
    }
  }, 20);
});
$(window).on("resize", function () {
  $("body").css("padding-top", navbarHeight);
});
$("body").on("click", ".tabs li", function () {
  $(this).addClass("active").siblings().removeClass("active");
  $($(this).data("class")).show().siblings().hide();
  if ($(this).hasClass("seriesList")) {
    if ($("section.seriesList .Small--Box").length == 0) {
      $.ajax({
        url: MyAjaxURL + "series.php",
        type: "POST",
        data: { id: $(this).data("id") },
        success: function (data) {
          $("section.seriesList").html(data);
        },
      });
    } else {
    }
  }
});
jQuery(document).ready(function ($) {
  $(".tabs li").first().addClass("active").siblings().removeClass("active");
  $($(".tabs li").first().data("class")).show().siblings().hide();
});
$("body").on(
  "click",
  ".watch--servers--list li.server--item",
  function (event) {
    event.preventDefault();
    $(this).addClass("active").siblings().removeClass("active");
    $.ajax({
      url: MyAjaxURL + "Single/Server.php",
      type: "POST",
      data: { id: $(this).data("id"), i: $(this).data("server") },
      success: function (data) {
        $(".player--iframe").html(data);
      },
    });
  }
);
$("body").on("click", ".downloadBTN", function (event) {
  $(".downloads").slideToggle();
});
$("body").on("click", ".close--pop", function (event) {
  event.preventDefault();
  $(".trailer--popup").removeClass("active");
  $(".trailer--popup .pop--iframe").html("");
});
$("body").on("click", ".WatcHTrailer", function (event) {
  event.preventDefault();
  $(".trailer--popup").addClass("active");
  $(".trailer--popup .pop--iframe").html("<h5>انتظر لحظة ....</h5>");
  $.ajax({
    url: MyAjaxURL + "Single/Trailer.php",
    type: "POST",
    data: { id: $(this).data("id") },
    success: function (data) {
      $(".trailer--popup .pop--iframe").html(data);
    },
  });
});
$("body").on(
  "keyup",
  '.SearchPopForm form.Header--Search-Form input[type="text"]',
  function (event) {
    event.preventDefault();
    Searching($(this));
  }
);
$("body").on(
  "change",
  ".SearchPopForm form.Header--Search-Form select#types",
  function (event) {
    Searching($('.SearchPopForm form.Header--Search-Form input[type="text"]'));
  }
);
$(window).scroll(function (event) {
  if ($(window).scrollTop() > 500) {
    $(".gtop").addClass("show");
  } else {
    $(".gtop").removeClass("show");
  }
});
$("body").on("click", ".gtop", function (event) {
  event.preventDefault();
  $("body,html").animate({ scrollTop: "0" }, 1000);
});
$("body").on(
  "click",
  "section.Footer--Filter .Dropdown--Button",
  function (event) {
    $(".Advanced--Filter").slideToggle();
  }
);
$("body").on("click", ".List--Of--Terms li", function () {
  $(this).toggleClass("active");
  $("." + $(this).parent("class") + " li").each(function () {
    if ($(this).hasClass("active")) {
    }
  });
});
$("body").on("click", '.Advanced--Filter input[type="submit"]', function (e) {
  e.preventDefault();
  $(".Footer--Filter form button").click();
});
$("body").on("click", ".seasons--toggler h3", function (event) {
  event.preventDefault();
  $(".seasons--toggler").toggleClass("active");
  $(".seasons--toggler ul").slideToggle(200);
});
$("body").on("click", ".seasons--toggler ul li a", function (event) {
  event.preventDefault();
  $(".episodes--list--side").html(
    '<div class="moreLoader"><div class="showbox"><div class="loader"><svg class="circular" viewBox="25 25 50 50"><circle class="path" cx="50" cy="50" r="20" fill="none" stroke-width="2" stroke-miterlimit="10"/></svg></div></div></div>'
  );
  $(".seasons--toggler li").removeClass("active");
  $(this).parent().addClass("active");
  $(".seasons--toggler h3 span").html($(this).html());
  $(".seasons--toggler ul").slideUp();
  $.ajax({
    url: MyAjaxURL + "Single/Episodes.php",
    type: "POST",
    dataType: "html",
    data: { season: $(this).data("season"), post_id: $(this).data("id") },
    success: function (requesetResponse) {
      $(".episodes--list--side").html(requesetResponse);
    },
  });
});
$("body").on("click", ".mobile--bars", function () {
  $("body").addClass("MenuOn");
});
$("body").on("click", ".MainMenuOverlay", function () {
  $("body").removeClass("MenuOn");
});
$("body").on("click", ".search--toggle", function () {
  $("body").toggleClass("searchOn");
});
$("body").on("click", ".mobile--search", function () {
  $("body").toggleClass("searchOn");
});
$("body").on("click", ".Header--Search", function () {
  $("body").toggleClass("searchPopOn");
});
$("body").on("click", ".filterPosts #filterForm > ul > li > span", function () {
  $(this).parent().siblings().removeClass("active");
  $(this).parent().toggleClass("active");
});
$("body").on(
  "click",
  ".filterPosts #filterForm > ul > li > ul > li",
  function () {
    $(this).addClass("active").siblings().removeClass("active");
    $("#filterForm input[name=" + $(this).data("tax") + "]").val(
      $(this).data("term")
    );
    $(this).parent().parent().find(".current").text($(this).data("name"));
    $(".filterPosts #filterForm > ul > li").removeClass("active");
  }
);
$("body").on("submit", "#filterForm", function (event) {
  event.preventDefault();
  $(".Posts--List").addClass("isLoading").html(loader);
  $(".filterPosts #filterForm > ul > li").removeClass("active");
  $(".paginate").hide();
  $.ajax({
    url: HomeURL + "/ajaxCenter/",
    type: "POST",
    data: $("#filterForm").serialize(),
  }).done(function (response) {
    $(".Posts--List").removeClass("isLoading").html(response);
  });
  return !1;
});
$("body").on("click", ".SearchPopForm button#search--close", function (event) {
  event.preventDefault();
  $("body").removeClass("searchPopOn");
  $('.SearchPopForm form.Header--Search-Form input[type="text"]').val("");
});
